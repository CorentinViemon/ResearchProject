package C3M;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * This class permits to represent a Knowledge Base.
 * Indeed, all relations are downloading and the first level too.
 * 
 * 
 * @author corentinviemon
 *
 */
public class Data_Retrieve {	
	
	public Triplestore triplestore;
	protected String graph = "<http://dbpedia.org>";
	

	private int level = 1;
	public float delta ;
	public double minLikelihood;	//likelihood threshold
	public double minOccurrence;
	public int maximumCardinality = Integer.MAX_VALUE;
	public long startTime; //to manage execution time
	
	public Map <Integer, ArrayList<String>> levelExplore;
	public ArrayList <String> relationsList;
	
	
	
	/**
	 * Constructor
	 * 
	 * 
	 * @param triplestore
	 * @param confidenceLevel
	 * @param minLikelihood
	 * @throws InterruptedException
	 */
	public Data_Retrieve(Triplestore triplestore, double confidenceLevel, double minLikelihood) throws InterruptedException {
		this.triplestore = triplestore;
		this.minLikelihood = minLikelihood;
		this.delta = (float) (1 - confidenceLevel);
		this.relationsList = new ArrayList<String>();
		this.levelExplore = new HashMap <Integer, ArrayList<String>>();
		minOccurrence = (int) this.getMinimalCount();
		
		//Print many information before starting
		System.out.println("Endpoint : " + triplestore.getEndpoint());
		System.out.println("Likelihood threshold: " + minLikelihood);
		System.out.println("Confidence level : " + confidenceLevel);
		System.out.println("Delta : " + delta);
		System.out.println("Minimum Occurrences : " + minOccurrence +"\r\n\r\n\r\n");
		
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF); //Desactivate Warning from Jena
		
		startTime = System.currentTimeMillis();
		System.out.println("******** Downloading relations ********");
		download_Relations();
		System.out.println(relationsList.size() + " relations downloaded in" + time() + "\r\n\r\n");
	}
	
	
	
	
	
	
	/**
	 * Retrieve all relations in the Knowledge Base.
	 * 
	 * @throws InterruptedException
	 */
	public void download_Relations() throws InterruptedException {
		//Query to retrieve all relations
		String queryStr = Triplestore.getPrefix()
				+ "SELECT ?relation (COUNT(?x) AS ?count) "
				+ "FROM " + graph
				+ "WHERE {?entity ?relation ?x} "
				+ "GROUP BY ?relation HAVING (COUNT(?x)>=" + minOccurrence + ") ORDER BY DESC(COUNT(?x))";
		
		ContextsRelationsQuery query = new ContextsRelationsQuery(queryStr, triplestore.getEndpoint(), "relation");
		query.execute();
		relationsList = query.resultsList;
	}
	
	
	
	
	
	
	/**
	 * Retrieve all contexts of the level One for a given relation.
	 * 
	 * @param relation
	 * @throws InterruptedException
	 */
	public void levelExploration(String relation) throws InterruptedException{
		this.levelExplore.clear();
		String queryStr;
		//To retrieve the level One
		queryStr = Triplestore.getPrefix()
				+ "SELECT distinct ?context FROM " + graph + " WHERE {\r\n" + 
				"?a a ?context. MINUS{?context rdfs:subClassOf ?x}. ?a <" + relation + "> ?o} GROUP BY ?context HAVING(COUNT(?a)>=" + minOccurrence + ")";
		ContextsRelationsQuery query = new ContextsRelationsQuery(queryStr, triplestore.getEndpoint(), "context");
		query.execute();
		
		//Results are put in a HashMap to retrieve results quickly during the rest of the algorithm
		this.levelExplore.put(level, query.resultsList);
		}
	
	
	
	
	/**
	 * Compute the minimum number of occurrences needed according to delta
	 * and the likelihood threshold.
	 * 
	 * @return the minimum number of occurences
	 */
	public double getMinimalCount() {
		double epsilon = 1 - minLikelihood;
		return 0.5 * Math.log(1 / delta) / (epsilon * epsilon);
	}
	
	
	/**
	 * Thus method permits to compute the elapsed time since the beginning.
	 * 
	 * @return a String to know the elapsed time
	 */
	public String time() {
		long endTime = System.currentTimeMillis(); //On récupère le temps
		long ms = endTime-startTime; //On calcule le temps écoulé
		ms=ms/1000; 
		long secondes=ms%60; 
		ms=ms/60; 
		long minutes=ms%60; 
		ms=ms/60; 
		long heures=ms;
		String time = " " + heures + "h" + minutes+ "min" + secondes +"s";
		return time;
	}
	
	
}
