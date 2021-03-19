package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {

    int batchsize; // Number of tuples per out batch
    ArrayList<Integer> leftindex; // Indices of the join attributes in left table
    ArrayList<Integer> rightindex; // Indices of the join attributes in right table

    Batch outputbatch; // Buffer page for output
    Batch leftbatch; // Buffer page for left input stream
    Batch rightbatch; // Buffer page for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // Whether end of stream (right table) is reached

    ArrayList<Attribute> rightAttributes;
    ArrayList<Attribute> leftAttributes;

    ExternalSort sortedLeft;
    ExternalSort sortedRight;

    Tuple leftTuple = null; // The left tuple that is being read from left input page
    Tuple rightTuple = null; // The right tuple that is being read from right input page
    Tuple nextLeftTuple = null; // The next left tuple from current left tuple
    Tuple nextTuple = null; // The next right tuple from current right tuple

    ArrayList<Tuple> rightPartition; // The rightPartition that is being joined
    int partitionEosr;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        rightindex = new ArrayList<>();
        leftindex = new ArrayList<>();
        rightAttributes = new ArrayList<>();
        leftAttributes = new ArrayList<>();

        for (int i = 0; i < conditionList.size(); i ++ ) {
            Condition con = conditionList.get(i);

            Attribute rightattr = (Attribute) con.getRhs();
            Attribute leftattr = con.getLhs();

            rightAttributes.add(rightattr);
            leftAttributes.add(leftattr);

            rightindex.add(right.getSchema().indexOf(rightattr));
            leftindex.add(left.getSchema().indexOf(leftattr));
        }

        sortedRight = new ExternalSort(right, rightAttributes, numBuff);
        sortedRight.open();

        sortedLeft = new ExternalSort(left, leftAttributes, numBuff);
        sortedLeft.open();

        return true;
    }
    /**
     * Perform Merging process
     * Read left page and then read the next tuple from the left page
     * Read 1 partition from right table which has the set join key of the same values. After which it read in a tuple from the right page.
     * Then it does a comparison of their join key till equal values are found.
     * Add the join result and return the output page when it is full.
     */
    public Batch next() {
        if (eosl || eosr) {
            close();
            return null; // Return null if either left or right table reach the end of stream
        }

        if (leftbatch == null) {
            leftbatch = sortedLeft.next();
            if (leftbatch.isEmpty() || leftbatch == null) {
                eosl = true;
                return null;
            }
            leftTuple = leftbatch.get(lcurs);
        }

        if (rightbatch == null) {
            rightbatch = sortedRight.next();
            if (rightbatch.isEmpty() || rightbatch == null ) {
                eosr = true;
                return outputbatch;
            }

            rightPartition = getNextPartition();

            if (checkPartitionSize(rightPartition)) {
                System.exit(0);
            }
            partitionEosr = 0;
            rightTuple = rightPartition.get(0);
        }

        Batch outputBatch = new Batch(batchsize);
        while (!outputBatch.isFull()) {
            int compRe = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);
            // if both left and right are equal
            if (compRe == 0) {
                outputBatch.add(leftTuple.joinWith(rightTuple)); // add join result into outputBatch and continues with the next comparison
                if (partitionEosr < rightPartition.size() - 1) {
                    partitionEosr++;
                    rightTuple =  rightPartition.get(partitionEosr);
                }
                else if (partitionEosr == rightPartition.size() - 1) {
                    lcurs++;
                    if (lcurs == leftbatch.size()) {
                        leftbatch = sortedLeft.next();
                        if (leftbatch == null || leftbatch.isEmpty()) {
                            eosl = true;
                            break;
                        }
                        lcurs = 0;
                    }
                    nextLeftTuple = leftbatch.get(lcurs);
                    if (nextLeftTuple == null) {
                        eosl = true;
                        break;
                    }
                    compRe = Tuple.compareTuples(nextLeftTuple, leftTuple, leftindex, leftindex);
                    leftTuple = nextLeftTuple;
                    // check if two consecutive tuples have the same values
                    if (compRe == 0) {
                        partitionEosr = 0;
                        rightTuple = rightPartition.get(partitionEosr);
                    } else {
                        rightPartition = getNextPartition();
                        if (rightPartition.size() == 0 || rightPartition == null ) {
                            eosr = true;
                            break;
                        }
                        partitionEosr = 0;
                        rightTuple = rightPartition.get(partitionEosr);
                    }
                }
            }
            else if (compRe > 0) {
                rightPartition = getNextPartition();
                if (rightPartition.size() == 0 || rightPartition.isEmpty() || rightPartition == null) {
                    eosr = true;
                    break;
                }
                partitionEosr = 0;
                rightTuple = rightPartition.get(partitionEosr);
            }
            else if (compRe < 0) {
                lcurs++;
                if (lcurs == leftbatch.size()) {
                    leftbatch = sortedLeft.next();
                    if (leftbatch == null || leftbatch.isEmpty()) {
                        eosl = true;
                        break;
                    }
                    lcurs = 0;
                }
                leftTuple = leftbatch.get(lcurs);
            }
        }
        return outputBatch;
    }

    /**
     * Continuous  read in the right tuples until a change in values
     * @return right partition which is to be compare with the left tuple
     */
    private ArrayList<Tuple> getNextPartition() {
        ArrayList<Tuple> partition = new ArrayList<Tuple>();
        int comparisionRes = 0;

        if (rightbatch.isEmpty() || rightbatch == null) {
            return partition;
        }

        if (nextTuple == null) {
            if (rcurs == rightbatch.size()) {
                rightbatch = sortedRight.next();
                if (rightbatch == null) {
                    eosr = true;
                    if (checkPartitionSize(partition)) {
                        System.exit(0);
                    }
                    return partition;
                }
                rcurs = 0;
            }
            nextTuple = rightbatch.get(rcurs);
            if (nextTuple == null) {
                return partition;
            }
            rcurs++;
        }

        while (comparisionRes == 0) {
            partition.add(nextTuple);

            if (rcurs == rightbatch.size()) {
                rightbatch = sortedRight.next();
                if (rightbatch.isEmpty() || rightbatch == null) {
                    eosr = true;
                    return partition;
                }
                rcurs = 0;
            }
            nextTuple = rightbatch.get(rcurs);
            rcurs++;
            comparisionRes = Tuple.compareTuples(partition.get(0), nextTuple, rightindex, rightindex);
        }

        // terminate the process if buffer size smaller then partition size
        if (checkPartitionSize(partition)) {
            System.exit(0);
        }
        return partition;
    }

    /**
     * Check if the partition size is large than the buffer
     * @param partition
     * @return result of the status of check PartitionSize
     */
    private boolean checkPartitionSize(ArrayList<Tuple> partition) {
        int numTuples = batchsize * (numBuff - 2);
        if (partition.size() > numTuples) {
            System.out.println("Buffer size is smaller then partition size, please try a larger buffer size");
            return true; // Stop execution
        }
        return false;
    }

    // Close join
    public boolean close() {
        sortedLeft.close();
        sortedRight.close();
        return true;
    }
}
