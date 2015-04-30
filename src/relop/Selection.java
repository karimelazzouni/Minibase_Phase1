package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class Selection extends Iterator {

	private Iterator iterator;
	private Predicate preds [];
	private Tuple next_tuple;
  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  public Selection(Iterator iter, Predicate... preds) {
	  this.iterator = iter;
	  setSchema(this.iterator.getSchema());
	  this.preds = preds;
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
	  this.iterator.restart();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
	  return this.iterator.isOpen();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
	  this.iterator.close();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
	  if (next_tuple != null)
		  return true;
	  next_tuple = this.getNextTuple();
	  return next_tuple != null;
//    throw new UnsupportedOperationException("Not implemented");
  }
  
  private Tuple getNextTuple() {
	  while (iterator.hasNext()) {
		  Tuple t = this.iterator.getNext();
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
	  return null;
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if (hasNext()) {
		  Tuple temp = this.next_tuple;
		  next_tuple = null;
		  return temp;
	  }
	  throw new IllegalStateException();
//    throw new UnsupportedOperationException("Not implemented");
  }

} // public class Selection extends Iterator
