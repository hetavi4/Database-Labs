package simpledb.storage;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    private final int tableId;
    private final int pageNumber;

    /**
     * Constructor.
     */
    public HeapPageId(int tableId, int pgNo) {
        this.tableId = tableId;
        this.pageNumber = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        return tableId;
    }

    /**
     * @return the page number in the table
     */
    public int getPageNumber() {
        return pageNumber;
    }

    /**
     * hashCode implementation
     */
    @Override
    public int hashCode() {
        int result = Integer.hashCode(tableId);
        result = 31 * result + Integer.hashCode(pageNumber);
        return result;
    }

    /**
     * equals implementation
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HeapPageId)) return false;

        HeapPageId other = (HeapPageId) o;
        return this.tableId == other.tableId &&
               this.pageNumber == other.pageNumber;
    }

    /**
     * Serialize for disk
     */
    public int[] serialize() {
        int[] data = new int[2];
        data[0] = tableId;
        data[1] = pageNumber;
        return data;
    }
}