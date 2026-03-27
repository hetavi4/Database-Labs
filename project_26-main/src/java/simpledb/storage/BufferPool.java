package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    private final int numPages;
    // LinkedHashMap with access order = true gives us LRU ordering
    private final LinkedHashMap<PageId, Page> pageMap;
    private final LockManager lockManager;

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new LinkedHashMap<>(numPages, 0.75f, true);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

        // Acquire the lock first — may block until granted (uses LockManager's own monitor)
        lockManager.acquireLock(tid, pid, perm);

        synchronized (this) {
            if (pageMap.containsKey(pid)) {
                return pageMap.get(pid);
            }
            if (pageMap.size() >= numPages) {
                evictPage();
            }
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page = file.readPage(pid);
            pageMap.put(pid, page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     */
    public void transactionComplete(TransactionId tid) {
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.insertTuple(tid, t);
        synchronized (this) {
            for (Page p : dirtyPages) {
                p.markDirty(true, tid);
                pageMap.put(p.getId(), p);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = file.deleteTuple(tid, t);
        synchronized (this) {
            for (Page p : dirtyPages) {
                p.markDirty(true, tid);
                pageMap.put(p.getId(), p);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * Does NOT remove pages from the buffer pool.
     */
    public synchronized void flushAllPages() throws IOException {
    for (PageId pid : new java.util.ArrayList<>(pageMap.keySet())) {
        flushPage(pid);
    }
}

    /** Remove the specific page id from the buffer pool without flushing. */
    public synchronized void discardPage(PageId pid) {
        pageMap.remove(pid);
    }

    /**
     * Flushes a dirty page to disk and marks it as not dirty.
     * Does NOT remove the page from the buffer pool.
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageMap.get(pid);
        if (page == null) return;
        if (page.isDirty() != null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk. */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // not necessary for lab1|lab2
    }

    /**
     * Tracks shared and exclusive page-level locks per transaction.
     * All methods are synchronized on the LockManager instance, so it is safe
     * to call acquireLock() from outside any BufferPool synchronized block
     * (avoiding a deadlock where a thread holds the BufferPool monitor while
     * waiting for a lock that another thread cannot release because it too is
     * blocked on the BufferPool monitor).
     */
    private class LockManager {
        // pageId -> set of transactions holding a shared lock
        private final Map<PageId, Set<TransactionId>> sharedLocks = new HashMap<>();
        // pageId -> transaction holding the exclusive lock (at most one)
        private final Map<PageId, TransactionId> exclusiveLocks = new HashMap<>();

        public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException {
            if (perm == Permissions.READ_ONLY) {
                acquireSharedLock(tid, pid);
            } else {
                acquireExclusiveLock(tid, pid);
            }
        }

        private void acquireSharedLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            while (true) {
                TransactionId excHolder = exclusiveLocks.get(pid);
                // Grant if nobody holds exclusive, or WE hold exclusive (already have stronger lock)
                if (excHolder == null || excHolder.equals(tid)) {
                    sharedLocks.computeIfAbsent(pid, k -> new HashSet<>()).add(tid);
                    return;
                }
                try { wait(); } catch (InterruptedException e) { throw new TransactionAbortedException(); }
            }
        }

        private void acquireExclusiveLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            while (true) {
                TransactionId excHolder = exclusiveLocks.get(pid);
                // Already hold exclusive — nothing to do
                if (tid.equals(excHolder)) return;

                Set<TransactionId> shared = sharedLocks.getOrDefault(pid, Collections.emptySet());
                boolean noExclusive = (excHolder == null);
                // Can grant if no other transaction holds shared (upgrade ok if only we hold shared)
                boolean noOtherShared = shared.isEmpty() || (shared.size() == 1 && shared.contains(tid));

                if (noExclusive && noOtherShared) {
                    exclusiveLocks.put(pid, tid);
                    return;
                }
                try { wait(); } catch (InterruptedException e) { throw new TransactionAbortedException(); }
            }
        }

        public synchronized void releaseLock(TransactionId tid, PageId pid) {
            if (tid.equals(exclusiveLocks.get(pid))) {
                exclusiveLocks.remove(pid);
            }
            Set<TransactionId> shared = sharedLocks.get(pid);
            if (shared != null) {
                shared.remove(tid);
                if (shared.isEmpty()) sharedLocks.remove(pid);
            }
            notifyAll();
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            if (tid.equals(exclusiveLocks.get(pid))) return true;
            Set<TransactionId> shared = sharedLocks.get(pid);
            return shared != null && shared.contains(tid);
        }
    }

    /**
     * Evicts the least recently used page from the buffer pool using LRU policy.
     * Flushes the page first if it is dirty.
     */
    private synchronized void evictPage() throws DbException {
        // Iterate in LRU order (first entry = least recently used)
        for (Map.Entry<PageId, Page> entry : pageMap.entrySet()) {
            PageId pid = entry.getKey();
            try {
                flushPage(pid);
            } catch (IOException e) {
                throw new DbException("Failed to flush page during eviction: " + e.getMessage());
            }
            pageMap.remove(pid);
            return;
        }
        throw new DbException("Buffer pool is full and no page could be evicted");
    }

}
