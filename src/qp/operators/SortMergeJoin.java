package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {

    static int filenum = 0; // To get unique filenum for this operation
    int batchsize; // Number of tuples per out batch
    ArrayList<Integer> leftindex; // Indices of the join attributes in left table
    ArrayList<Integer> rightindex; // Indices of the join attributes in right table
    String rfname; // The file name where the right table is materialized
    Batch outbatch; // Buffer page for output
    Batch leftbatch; // Buffer page for left input stream
    Batch rightbatch; // Buffer page for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int bcurs; // Cursor for buffer iterator
    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // Whether end of stream (right table) is reached

    ArrayList<Attribute> rightAttributes;
    ArrayList<Attribute> leftAttributes;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        rightindex = new ArrayList<>();
        leftindex = new ArrayList<>();
        rightAttributes = new ArrayList<>();
        leftAttributes = new ArrayList<>();

        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftAttributes.add(leftattr);
            rightAttributes.add(rightattr);

            rightindex.add(right.getSchema().indexOf(rightattr));
            leftindex.add(left.getSchema().indexOf(leftattr));
        }

        //implement sort function
        //Sort both left and right table

        return true;
    }
}
