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

public class BufferPool {

    private final int numPages;
    private final LinkedHashMap<PageId, Page> pageMap;
    private final LockManager lockManager;

    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    public static final int DEFAULT_PAGES = 50;

    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new LinkedHashMap<>(numPages, 0.75f, true);
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {

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

    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Exercise 4: commit always calls transactionComplete(tid, true)
     */
    public void transactionComplete(TransactionId tid) {
        try {
            transactionComplete(tid, true);
        } catch (Exception e) {
            // ignore
        }
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Exercise 4: commit or abort a transaction.
     * - commit: flush all dirty pages belonging to this transaction to disk (FORCE)
     * - abort:  discard dirty pages belonging to this transaction (NO STEAL — reload from disk)
     * Either way: release all locks held by this transaction.
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        synchronized (this) {
            for (PageId pid : new java.util.ArrayList<>(pageMap.keySet())) {
                Page page = pageMap.get(pid);
                if (page == null) continue;
                if (tid.equals(page.isDirty())) {
                    if (commit) {
                        // FORCE: write dirty pages to disk on commit
                        try {
                            flushPage(pid);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        // ABORT: discard dirty page — it will be reloaded from disk on next access
                        pageMap.remove(pid);
                    }
                }
            }
        }
        // Release all locks held by this transaction
        lockManager.releaseAllLocks(tid);
    }

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

    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : new java.util.ArrayList<>(pageMap.keySet())) {
            flushPage(pid);
        }
    }

    public synchronized void discardPage(PageId pid) {
        pageMap.remove(pid);
    }

    private synchronized void flushPage(PageId pid) throws IOException {
        Page page = pageMap.get(pid);
        if (page == null) return;
        if (page.isDirty() != null) {
            DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, null);
        }
    }

    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (PageId pid : new java.util.ArrayList<>(pageMap.keySet())) {
            Page page = pageMap.get(pid);
            if (page != null && tid.equals(page.isDirty())) {
                flushPage(pid);
            }
        }
    }

    /**
     * Exercise 3: NO STEAL eviction policy.
     * Never evict a dirty page. Only evict clean pages.
     * If all pages are dirty, throw DbException.
     */
    private synchronized void evictPage() throws DbException {
        for (Map.Entry<PageId, Page> entry : new java.util.ArrayList<>(pageMap.entrySet())) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();
            // NO STEAL: skip dirty pages
            if (page.isDirty() != null) {
                continue;
            }
            // Found a clean page — evict it
            pageMap.remove(pid);
            return;
        }
        throw new DbException("All pages in buffer pool are dirty — cannot evict (NO STEAL policy)");
    }

    private class LockManager {
        private final Map<PageId, Set<TransactionId>> sharedLocks = new HashMap<>();
        private final Map<PageId, TransactionId> exclusiveLocks = new HashMap<>();
        // track all pages locked by each transaction (for releaseAllLocks)
        private final Map<TransactionId, Set<PageId>> transactionPages = new HashMap<>();

        public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
                throws TransactionAbortedException {
            if (perm == Permissions.READ_ONLY) {
                acquireSharedLock(tid, pid);
            } else {
                acquireExclusiveLock(tid, pid);
            }
            // Track which pages this transaction holds locks on
            transactionPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
        }

        private void acquireSharedLock(TransactionId tid, PageId pid)
                throws TransactionAbortedException {
            while (true) {
                TransactionId excHolder = exclusiveLocks.get(pid);
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
                if (tid.equals(excHolder)) return;

                Set<TransactionId> shared = sharedLocks.getOrDefault(pid, Collections.emptySet());
                boolean noExclusive = (excHolder == null);
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
            Set<PageId> pages = transactionPages.get(tid);
            if (pages != null) pages.remove(pid);
            notifyAll();
        }

        /**
         * Release ALL locks held by a transaction (called on commit/abort).
         */
        public synchronized void releaseAllLocks(TransactionId tid) {
            Set<PageId> pages = transactionPages.remove(tid);
            if (pages == null) return;
            for (PageId pid : pages) {
                if (tid.equals(exclusiveLocks.get(pid))) {
                    exclusiveLocks.remove(pid);
                }
                Set<TransactionId> shared = sharedLocks.get(pid);
                if (shared != null) {
                    shared.remove(tid);
                    if (shared.isEmpty()) sharedLocks.remove(pid);
                }
            }
            notifyAll();
        }

        public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
            if (tid.equals(exclusiveLocks.get(pid))) return true;
            Set<TransactionId> shared = sharedLocks.get(pid);
            return shared != null && shared.contains(tid);
        }
    }
}
