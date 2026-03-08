package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator aggIterator;
    private TupleDesc td;

    /**
     * Constructor.
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return null;
     */
    public String groupFieldName() {
        if (gfield == Aggregator.NO_GROUPING) return null;
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();

        TupleDesc childTd = child.getTupleDesc();
        Type gbfieldtype = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);
        Type afieldtype = childTd.getFieldType(afield);

        if (afieldtype == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
        }

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();

        aggIterator = aggregator.iterator();
        aggIterator.open();

        // Build output TupleDesc
        String aggName = aop.toString() + "(" + childTd.getFieldName(afield) + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggName});
        } else {
            td = new TupleDesc(
                new Type[]{gbfieldtype, Type.INT_TYPE},
                new String[]{childTd.getFieldName(gfield), aggName}
            );
        }
    }

    /**
     * Returns the next tuple from the aggregate iterator, or null if done.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIterator != null && aggIterator.hasNext()) {
            return aggIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     */
    public TupleDesc getTupleDesc() {
        if (td != null) return td;
        TupleDesc childTd = child.getTupleDesc();
        Type gbfieldtype = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);
        String aggName = aop.toString() + "(" + childTd.getFieldName(afield) + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggName});
        } else {
            return new TupleDesc(
                new Type[]{gbfieldtype, Type.INT_TYPE},
                new String[]{childTd.getFieldName(gfield), aggName}
            );
        }
    }

    public void close() {
        super.close();
        if (aggIterator != null) {
            aggIterator.close();
            aggIterator = null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
