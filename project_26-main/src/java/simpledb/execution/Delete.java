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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
 
    private static final long serialVersionUID = 1L;
 
    private final TransactionId tid;
    private OpIterator child;
    private final TupleDesc resultTd;
    private boolean called; // fetchNext should only return a result once
 
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.child = child;
        this.called = false;
        // The output is always a single integer field: the count of deleted tuples
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                Database.getBufferPool().deleteTuple(tid, t);
                count++;
            } catch (IOException e) {
                throw new DbException("Delete failed: " + e.getMessage());
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
