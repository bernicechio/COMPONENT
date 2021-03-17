package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;

/**
 * External Sort Algorithm
 */
public class ExternalSort extends Operator {
    // The number of buffer pages available
    private final int numBuff;
    // The base operator
    private final Operator base;
    // The number of tuples per batch
    private final int batchSize;
    // A random string generated to differentiate instances of external sort operators
    private final String randomID = UUID.randomUUID().toString();
    // The index of the attributes to sort based on
    private final ArrayList<Integer> indexOfAttributes = new ArrayList<>();
    // The input stream to read the sorted result
    private ObjectInputStream resultOfSort;
    // Notify if the out-of-stream result is reached for sorting
    private boolean eos = false;

    /**
     * Creates a new external sort operator
     * @param base: the base operator
     * @param attributes: the list of attributes
     * @param numBuff: the number of buffers available
     */
    public ExternalSort(Operator base, ArrayList attributes, int numBuff) {
        super(OpType.SORT);
        this.base = base;
        this.schema = base.schema;
        this.numBuff = numBuff;
        this.batchSize = Batch.getPageSize() / schema.getTupleSize();

        for (int i = 0; i < attributes.size(); i++) {
            Attribute attribute = (Attribute) attributes.get(i);
            indexOfAttributes.add(schema.indexOf(attribute));
        }
    }

    @Override
    public boolean open() {
        if (!base.open()) {
            return false;
        }
        int numOfRuns = generateSortedRuns();
        return (mergeSortedRuns(1, numOfRuns) == 1);
    }

    /**
     * generate sorted runs and store them into the disk
     * @return the number of runs generated
     */
    public int generateSortedRuns() {
        Batch in = base.next();

        int numOfRuns = 0;
        while (in != null) {
            // All the tuples to be sorted for this run
            ArrayList<Tuple> tuples = new ArrayList<>();

            for (int i = 0; i < numBuff && in != null; i++) {
                tuples.addAll(in.getAllTuples());
                if (i != numBuff - 1) {
                    in = base.next();
                }
            }

            tuples.sort(this::isEqual);

            // phase 1 is the 0th pass
            String fileName = fileName(0, numOfRuns);
            try {
                ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(fileName));
                for (Tuple tuple: tuples) {
                    stream.writeObject(tuple);
                }
                stream.close();
            }  catch (IOException e) {
                System.err.println("external sort: cannot write sorted runs");
                System.exit(1);
            }

            in = base.next();
            numOfRuns += 1;
        }
        return numOfRuns;
    }

    /**
     * Merge the sorted runs generated
     * @param numOfPass : the number of passes
     * @param numOfRuns : the number of sorted runs
     * @return 1 : signals the end of merging process
     */
    public int mergeSortedRuns(int numOfPass, int numOfRuns) {
        if (numOfRuns <= 1) {
            try {
                String fileName = fileName(numOfPass - 1, numOfRuns - 1);
                resultOfSort = new ObjectInputStream(new FileInputStream(fileName));
            } catch (IOException e) {
                System.err.println("external sort: cannot create sorted result");
            }
            return numOfRuns;
        }

        int numOfOutput = 0;
        for (int start = 0; start < numOfRuns; start += numBuff - 1) {
            int end = Math.min(start + numBuff - 1, numOfRuns);
            try {
                mergeSubRuns(start, end, numOfPass, numOfOutput);
            } catch (ClassNotFoundException e) {
                System.err.println("external sort: class not found");
                System.exit(1);
            } catch (IOException e) {
                System.err.println("external sort: cannot merge runs");
                System.exit(1);
            }
            numOfOutput += 1;
        }
        return  mergeSortedRuns(numOfPass + 1, numOfOutput);
    }

    /**
     * Merge all the sorted run generated in between
     */
    private void mergeSubRuns(int start, int end, int passNum, int out) throws ClassNotFoundException, IOException {
        Batch[] inBatches = new Batch[end - start];
        ObjectInputStream[] inStreams = new ObjectInputStream[end - start];
        boolean[] inEos = new boolean[end - start];
        for (int i = start; i < end; i++) {
            String file = fileName(passNum - 1, i);
            ObjectInputStream inStream = new ObjectInputStream(new FileInputStream(file));
            Batch inBatch = new Batch(batchSize);

            while (!inBatch.isFull()) {
                try {
                    Tuple tuple = (Tuple) inStream.readObject();
                    inBatch.add(tuple);
                } catch (EOFException e) {
                    break;
                }
            }
            inBatches[i - start] = inBatch;
        }

        PriorityQueue<TupleForExternalSort> pq = new PriorityQueue<>(batchSize, (t1, t2) -> isEqual(t1.tuple, t2.tuple));
        String file = fileName(passNum, out);
        ObjectOutputStream outStream = new ObjectOutputStream(new FileOutputStream(file));

        for (int i = 0; i < end - start; i++) {
            Batch inBatch = inBatches[i];
            if (inBatch == null || inBatch.isEmpty()) {
                inEos[i] = true;
                continue;
            }
            Tuple curr = inBatch.get(0);
            pq.add(new TupleForExternalSort(curr, i, 0));
        }

        while (!pq.isEmpty()) {
            TupleForExternalSort outTuple = pq.poll();
            outStream.writeObject(outTuple.tuple);

            int nextBatchID = outTuple.sortedRunNum;
            int nextTupleID = outTuple.tupleNum + 1;
            if (nextTupleID == batchSize) {
                Batch inBatch = new Batch(batchSize);
                while (!inBatch.isFull()) {
                    try {
                        Tuple tuple = (Tuple) inStreams[nextBatchID].readObject();
                    } catch (EOFException e) {
                        break;
                    }
                }
                inBatches[nextBatchID] = inBatch;
                nextTupleID = 0;
            }
            Batch inBatch = inBatches[nextBatchID];
            if (inBatch == null || nextTupleID >= inBatch.size()) {
                inEos[nextBatchID] = true;
                continue;
            }
            Tuple next = inBatch.get(nextTupleID);
            pq.add(new TupleForExternalSort(next, nextBatchID, nextTupleID));
        }

        for (ObjectInputStream inStream : inStreams) {
            inStream.close();
        }
        outStream.close();
    }


    @Override
    public Batch next() {
        if (eos) {
            close();
            return null;
        }
        Batch out = new Batch(batchSize);
        while (!out.isFull()) {
            try {
                Tuple tuple = (Tuple) resultOfSort.readObject();
                out.add(tuple);
            } catch (ClassNotFoundException e) {
                System.err.println("Cannot read from sorted run");
                System.exit(1);
            } catch (EOFException e) {
                eos = true;
                return out;
            } catch (IOException e) {
                System.err.println("cannot read from sorted run");
            }
        }
        return out;
    }

    @Override
    public boolean close() {
        super.close();
        try {
            resultOfSort.close();
        } catch (IOException e) {
            System.err.println("cannot close sorted result");
            return false;
        }
        return true;
    }

    private String fileName(int passNum, int runNum) {
        return "harddisk-sorted-run-" + randomID + "-" + passNum + "-" + runNum;
    }

    // compare if the attributes of the two tuples are equal
    private int isEqual(Tuple t1, Tuple t2) {
        for (int i : indexOfAttributes) {
            int result = Tuple.compareTuples(t1, t2, i);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }
}
