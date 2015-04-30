package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {

	private Iterator left, right;
	private Predicate preds [];
	private Tuple cur_left_tuple;
	private Tuple next_tuple;
  /**
   * Constructs a join, given the left and right iterators and join predicates
   * (relative to the combined schema).
   */
  public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
	  this.left = left;
	  this.right = right;
	  
	  this.setSchema(Schema.join(left.getSchema(), right.getSchema()));
	  this.preds = preds;
	  
	  // TODO
	  cur_left_tuple = left.getNext(); // gets initial tuple
	  
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
	  this.left.restart();
	  this.right.restart();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return this.left.isOpen() && this.right.isOpen();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  this.left.close();
	  this.right.close();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if(this.next_tuple != null)
		  return true;
	  
	  this.next_tuple = getNextTuple();
	  return this.next_tuple != null;
	  //    throw new UnsupportedOperationException("Not implemented");
  }

  private Tuple getNextTuple() {
	  while (true) {
		  while (this.right.hasNext()) {
			  Tuple t = Tuple.join(cur_left_tuple, this.right.getNext(), this.getSchema());
			  boolean matched_all = true;
			  for (int i = 0; i < this.preds.length; i++) {
				  if (this.preds[i].evaluate(t) == false) {
					  matched_all = false;
					  break;
				  }
			  }
			  if (matched_all)
				  return t;
		  }
		  this.right.restart();
		  if (!this.left.hasNext())
			  break;
		  this.cur_left_tuple = this.left.getNext();
	  }
	  return null;
  }
  
  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if (this.hasNext()) {
		  Tuple temp = this.next_tuple;
		  this.next_tuple = null;
		  return temp;  
	  }
	  throw new IllegalStateException();	  
  }

} // public class SimpleJoin extends Iterator
