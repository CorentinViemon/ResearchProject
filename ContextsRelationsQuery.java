package C3M;

import java.util.ArrayList;

import org.apache.jena.query.QuerySolution;


/**
 * This class permits to run query to retrieve contexts or relation.
 * It extends the class SparqlQuerrier
 * 
 * 
 * @author corentinviemon
 *
 */
public class ContextsRelationsQuery extends SparqlQuerier{

	protected ArrayList <String> resultsList;
	protected boolean error = false;
	protected String solution; //To know if we want to retrieve contexts or relations in a query
	
	
	/**
	 * Constructor
	 * 
	 * @param query
	 * @param triplestore
	 * @param solution
	 */
	public ContextsRelationsQuery(String query, String triplestore, String solution) {
		super(query, triplestore);
		this.resultsList = new ArrayList <String>();
		this.solution = solution;
	}
	
	
	/**
	 * Constructor
	 * 
	 * @param query
	 * @param triplestore
	 * @param size
	 */
	public ContextsRelationsQuery(String query, String triplestore, int size) {
		super(query, triplestore);
		this.resultsList = new ArrayList <String>();
	}
	
	
	

	@Override
	public void begin() {
		
	}

	@Override
	public void end() {
		error=false;
	}

	
	/**
	 * This method permits to add a result in the ArrayList countPerCardinalities
	 */
	@Override
	public boolean fact(QuerySolution qs) throws InterruptedException {
		String temp = qs.get(solution).asResource().toString();
		resultsList.add(temp);
		return true;
	}

	
	/**
	 * To manage error in case of timeout or something else
	 */
	@Override
	public void error() {
		error = true;
		
	}

}
