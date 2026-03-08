package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    // Maps group value -> [aggregate, count] (count used only for AVG)
    private final LinkedHashMap<Field, long[]> groups;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int val = ((IntField) tup.getField(afield)).getValue();

        if (!groups.containsKey(groupVal)) {
            switch (what) {
                case MIN: groups.put(groupVal, new long[]{Long.MAX_VALUE, 0}); break;
                case MAX: groups.put(groupVal, new long[]{Long.MIN_VALUE, 0}); break;
                default:  groups.put(groupVal, new long[]{0, 0}); break;
            }
        }

        long[] agg = groups.get(groupVal);
        switch (what) {
            case COUNT: agg[0]++; break;
            case SUM:   agg[0] += val; break;
            case AVG:   agg[0] += val; agg[1]++; break;
            case MIN:   agg[0] = Math.min(agg[0], val); break;
            case MAX:   agg[0] = Math.max(agg[0], val); break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, long[]> entry : groups.entrySet()) {
            long[] agg = entry.getValue();
            int aggVal = (what == Op.AVG) ? (int)(agg[0] / agg[1]) : (int) agg[0];

            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(aggVal));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(aggVal));
            }
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
