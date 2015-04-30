package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

	private HeapScan hscan;
	private boolean isOpened;
	private HeapFile file;
	private RID next_rid;
  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
	  this.hscan  = file.openScan();
	  isOpened = true;
	  this.setSchema(schema);
	  this.file = file;
	  next_rid = new RID();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explanation of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  this.hscan = file.openScan();
	  isOpened = true;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return isOpened;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  hscan.close();
	  isOpened = false;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	boolean ret  = isOpened && hscan.hasNext();
	return ret;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  return new Tuple(this.getSchema(), hscan.getNext(next_rid));
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
	  return next_rid;
//    throw new UnsupportedOperationException("Not implemented");
  }

} // public class FileScan extends Iterator
