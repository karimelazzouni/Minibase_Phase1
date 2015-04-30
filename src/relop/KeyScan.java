package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

	private HashIndex hindex;
	private HashScan hscan;
	private SearchKey key;
	private HeapFile hfile;
	private boolean isOpened;
//	private RID next_rid;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
	  this.hscan = index.openScan(key);
	  this.isOpened = true;
	  this.setSchema(schema);
	  this.hindex = index;
	  this.hfile = file;
	  this.key = key;
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
	  this.hscan = hindex.openScan(key);
	  this.isOpened = true;
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
	  this.hscan.close();
	  this.isOpened = false;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return isOpened && hscan.hasNext();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  RID next_rid = this.hscan.getNext();
	  return new Tuple(this.getSchema(), hfile.selectRecord(next_rid));
//    throw new UnsupportedOperationException("Not implemented");
  }

} // public class KeyScan extends Iterator
