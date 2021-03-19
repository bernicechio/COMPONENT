package qp.operators;

import java.util.ArrayList;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

public class OrderBy extends Operator {

    Operator base;
    int batchSize; // No. of tuples per page
    int numBuff; //No. of buffer available
    Batch inputBatch = null; // Page read in memory
    ExternalSort sortedFiles;

    ArrayList<Attribute> orderAttSet; // Order Set of attribute list
    ArrayList<Integer> asIndices = new ArrayList<>(); //Attribute  list of schema to be ordered

    boolean eos = false; // Denote whether the end of table is reached
    boolean isDec;

    int inIndex = 0; // Tuple index from input

    public OrderBy(Operator base, ArrayList<Attribute> orderAttSet, boolean isDec) {
        super(OpType.ORDERBY);
        numBuff = BufferManager.getNumBuffer();
        this.base = base;
        this.orderAttSet = orderAttSet;
        this.isDec = isDec;
    }

    // get ready the output by calling the sort function to sort the table
    public boolean open() {
        for (Attribute att : orderAttSet)
            asIndices.add(schema.indexOf((att)));

        // call external sort function
        if (isDec) {
            sortedFiles = new ExternalSort(base, orderAttSet, numBuff, isDec);
        } else {
            sortedFiles = new ExternalSort(base, orderAttSet, numBuff);
        }
        sortedFiles.open();

        //set number of tuples per batch
        batchSize = Batch.getPageSize() / schema.getTupleSize();

        return true;
    }

    // Output the sorted result page by page
    public Batch next() {
        if (eos) {
            close();
            return null;
        } else if ( inputBatch == null) {
            inputBatch = sortedFiles.next(); // Get the next batch size from the sortFile
        }

        Batch outputBatch = new Batch(batchSize);
        // add inputBatch to outputbatch until it is full
        while (!outputBatch.isFull()) {
            if ( inputBatch.size() <= inIndex || inputBatch == null) {
                eos = true;
                break;
            }

            outputBatch.add(inputBatch.get(inIndex));
            inIndex++;

            if (inIndex == batchSize) {
                inputBatch = sortedFiles.next();
                inIndex = 0;
            }
        }
        return outputBatch;
    }

    public Object clone() {
        Operator cloneBase = (Operator) base.clone();
        ArrayList<Attribute> cloneOrderedList = new ArrayList<>();
        for(Attribute att : orderAttSet)
            cloneOrderedList.add(att);

        OrderBy cloneOrderBy = new OrderBy(cloneBase, cloneOrderedList, isDec);
        Schema cloneSchema = cloneBase.getSchema();
        cloneOrderBy.setSchema(cloneSchema);
        return cloneOrderBy;
    }

    public boolean close() {
        return sortedFiles.close();
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }
}
