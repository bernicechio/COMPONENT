package qp.operators;

import java.util.ArrayList;
import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

public class OrderBy extends Operator {

    Operator base;
    int batchSize; // No. of tuples per page
    int numOfBuffer; //No. of buffer available
    Batch inputBatch = null; // Page read in memory
    Sort sortedFiles;

    ArrayList<Attribute> orderAttSet; // Order Set of attribute list
    ArrayList<Integer> asIndices = new ArrayList<>(); //Attribute  list of schema to be ordered

    boolean eos = false; // Denote whether the end of table is reached

    int inIndex = 0; // Tuple index from input

    public OrderBy(Operator base, ArrayList<Attribute> orderAttSet) {
        super(OpType.ORDERBY);
        this.base = base;
        this.orderAttSet = orderAttSet;
        numOfBuffer = BufferManager.getNumBuffer();
    }

    public boolean open() {
        //set number of tuples per batch
        batchSize = Batch.getPageSize() / schema.getTupleSize();

        for (Attribute att : orderAttSet)
            asIndices.add(schema.indexOf((att)));

        // call sort function
        sortedFiles = new Sort(base, orderAttSet,numOfBuffer);
        sortedFiles.open();
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
        while (!outputBatch.isFull()) {
            if (inputBatch == null || inputBatch.size() <= inIndex) {
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

        OrderBy cloneOrderBy = new OrderBy(cloneBase, cloneOrderedList);
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
