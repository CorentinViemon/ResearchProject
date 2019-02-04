package C3M;

import java.util.ArrayList;
import org.apache.jena.query.QuerySolution;

/**
 * This class permits to run query to retrieve cardinalities.
 * It extends the class SparqlQuerrier
 * 
 * @author corentinviemon
 *
 */
public class CardinalityQuery extends SparqlQuerier {
	
	protected ArrayList<CountPerCardinality> countPerCardinalities; //groups cardinalities and their number of occurrences
	protected boolean error = false;

	
	/**
	 * Constructor
	 * 
	 * @param query
	 * @param triplestore
	 */
	public CardinalityQuery(String query, String triplestore) {
		super(query, triplestore);
		this.countPerCardinalities = new ArrayList<CountPerCardinality>();
	}
	
	
	/**
	 * Constructor
	 * 
	 * @param query
	 * @param triplestore
	 * @param size : the number of results needed
	 */
	public CardinalityQuery(String query, String triplestore, int size) {
		super(query, triplestore);
		this.countPerCardinalities = new ArrayList<CountPerCardinality>();
	}
	

	
	
	
	
	@Override
	public void begin() {
		// TODO Auto-generated method stub
	}

	@Override
	public void end() {
		
	}
	
	
	/**
	 * This method permits to add a result in the ArrayList countPerCardinalities
	 */
	@Override
	public boolean fact(QuerySolution qs) throws InterruptedException {
		
		CountPerCardinality card = new CountPerCardinality();
		card.cardinality = qs.get("cardinality").asLiteral().getInt();
		card.count = qs.get("count").asLiteral().getInt();
		this.countPerCardinalities.add(card);
		return true;
	}


	/**
	 * To manage error in case of timeout or something else
	 */
	@Override
	public void error() {
		error=true;
		
	}
	
	
}
