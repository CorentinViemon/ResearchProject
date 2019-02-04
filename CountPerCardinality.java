package C3M;


/**
 * This class permits to stock a cardinality and the number of occurrence
 * for this given cardinality
 * @author corentinviemon
 *
 */
public class CountPerCardinality {
	
	protected int cardinality;	
	protected int count;
		
	
	/**
	 * Constructor
	 */
	public CountPerCardinality() {
		this.cardinality = 0;
		this.count = 0;
	}
	
	
	/**
	 * Constructor
	 * @param cardinality
	 * @param count
	 */
	public CountPerCardinality(int cardinality, int count) {
		this.cardinality = cardinality;
		this.count = count;
	}
	
	
	
}
