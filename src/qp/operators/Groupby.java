package qp.operators;

import java.util.ArrayList;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;

public class Groupby extends Distinct {
    /**
     * Creates a new GROUP_BY operator.
     *
     * @param base is the base operator.
     */
    public Groupby(Operator base, ArrayList<Attribute> as, int type) {
        super(base, as, OpType.GROUPBY);
    }
}