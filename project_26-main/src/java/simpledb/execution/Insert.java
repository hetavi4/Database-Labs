package simpledb.execution;
 
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
 
import java.io.IOException;
 
/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {
 
    private static final long serialVersionUID = 1L;
 
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc resultTd;
    private boolean called; // fetchNext should return null if called more than once
 
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.called = false;
        // The output is always a single integer field: the count of inserted tuples
        this.resultTd = new TupleDesc(new Type[]{Type.INT_TYPE});
    }
 
    public TupleDesc getTupleDesc() {
        return resultTd;
    }
 
    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
        called = false;
    }
 
    public void close() {
        super.close();
        child.close();
    }
 
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        called = false;
    }
 
    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Return null if we've already returned the count tuple once
        if (called) {
            return null;
        }
        called = true;
 
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                count++;
            } catch (IOException e) {
                throw new DbException("Insert failed: " + e.getMessage());
            }
        }
 
        // Build and return the single result tuple containing the count
        Tuple result = new Tuple(resultTd);
        result.setField(0, new IntField(count));
        return result;
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
 
