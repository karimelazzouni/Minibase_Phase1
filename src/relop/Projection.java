package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

	private Iterator iterator;
	private Integer fields [];
	private Schema schema;
  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  public Projection(Iterator iter, Integer... fields) {
	  this.iterator = iter;
	  this.setSchema(iter.getSchema());
	  this.fields = fields;
	  this.schema = new Schema(fields.length);
	  for (int i = 0; i < fields.length; i++) {
		  this.schema.initField(i, this.iterator.getSchema(), fields[i]);
	}
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
	  return this.iterator.hasNext();
//    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
	  if (!this.iterator.hasNext())
		  throw new IllegalStateException();
	  Tuple original_t = this.iterator.getNext();
	  Tuple trunctated_t = new Tuple(this.schema);
	  
	  for (int i = 0; i < fields.length; i++) {
		trunctated_t.setField(i, original_t.getField(fields[i]));
	  }
	  
	  return trunctated_t;
//    throw new UnsupportedOperationException("Not implemented");
  }

} // public class Projection extends Iterator
