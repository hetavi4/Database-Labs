package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * To read a page from disk, you will first need to calculate the correct offset in the file. 
 * 
 * Hint: you will need random access to the file in order to read and write pages at arbitrary offsets.
 *  
 * You should not call BufferPool methods when reading a page from disk.
 *  
 * Do not load the entire table into memory on the open() call 
 * -- this will cause an out of memory error for very large tables.
 * 
 * At this point, your code should pass the unit tests in HeapFileReadTest.
 */

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    private final File file;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        int pageSize = BufferPool.getPageSize();
        long offset = (long) pgNo * pageSize;

        byte[] data = new byte[pageSize];

        try (RandomAccessFile raf = new RandomAccessFile(this.file, "r")) {
            raf.seek(offset);
            raf.readFully(data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not read page from disk", e);
        }

        try {
            return new HeapPage(new HeapPageId(tableId, pgNo), data);
        } catch (IOException e) {
            throw new IllegalArgumentException("Could not construct HeapPage", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil((double) this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    /** 
    * You will also need to implement the `HeapFile.iterator()` method, which should
    * iterate through the tuples of each page in the HeapFile.
    * 
    * The iterator must use the `BufferPool.getPage()` method to access pages in the `HeapFile`. 
    * This method loads the page into the buffer pool and will eventually be used (in a later lab)
    * to implement locking-based concurrency control and recovery. */
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    // inner class for HeapFileIterator using Iterator design pattern
    private class HeapFileIterator implements DbFileIterator {
        private final TransactionId tid;
        private int currentPageNo;
        private Iterator<Tuple> currentPageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.isOpen = false;
        }

        private Iterator<Tuple> getPageIterator(int pageNo) 
            throws TransactionAbortedException, DbException {
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(
                    tid, pid, Permissions.READ_ONLY);
            return page.iterator();
            }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            isOpen = true;
            currentPageNo = 0;
            if (currentPageNo < numPages()) {
                currentPageIterator = getPageIterator(currentPageNo);
            } else {
                currentPageIterator = Collections.emptyIterator();
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                return false;
            }

            // check if there are more more tuples in the page
            if (currentPageIterator != null && currentPageIterator.hasNext()) {
                return true;
            }

            //check if the next page has tuples if current page has no more tuples
            while (currentPageNo + 1 < numPages()) {
                currentPageNo++;
                currentPageIterator = getPageIterator(currentPageNo);
                if (currentPageIterator.hasNext()) {
                    return true;
                }
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                throw new NoSuchElementException("Iterator is not open");
            }
            if (!hasNext()) {
                throw new NoSuchElementException("No more tuples");
            }
            return currentPageIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!isOpen) {
                throw new DbException("Iterator is not open");
            }
            close();
            open();
        }

        @Override
        public void close() {
            isOpen = false;
            currentPageIterator = null;
        }
    }

}

