package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

	private BucketScan bscan;
	private HashIndex hindex;
	private HeapFile hfile;
	private boolean isOpened;
  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
	  this.bscan = index.openScan();
	  this.isOpened = true;
	  this.setSchema(schema);
	  this.hindex = index;
	  this.hfile = file;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
	  this.bscan = hindex.openScan();
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
	  this.bscan.close();
	  this.isOpened = false;
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  return isOpened && bscan.hasNext();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  RID next_rid = bscan.getNext();
	  return new Tuple(this.getSchema(), hfile.selectRecord(next_rid));
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
	  return bscan.getLastKey();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
	  return bscan.getNextHash();
//    throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
