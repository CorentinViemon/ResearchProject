package C3M;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class C3M_Algorithm {
	
	private Triplestore triplestore;
	private String graph;
	
	private String fileOut ; //String for the path of the output
	private FileWriter ffw ;
	private static int level;
	protected float delta ;
	protected double minLikelihood;	//Likelihood threshold
	protected double likelihood;
	protected double minOccurrence;
	protected int maximumCardinality = Integer.MAX_VALUE; //Infinite value
	
	
	
	private static Map <String, CardinalityConstraint> constraintsListToPrint = new HashMap<String, CardinalityConstraint>();
	private static Map <String,CardinalityConstraint> tempConstraintsList = new HashMap<String, CardinalityConstraint>();
	private static Map <String, ArrayList<String>> hierarchies = new HashMap<>();
	private static ArrayList<String> contextsDone = new ArrayList<String>();
	
	private Data_Retrieve K;
	
	
	/**
	 * This is the main class that permits to run the algorithm.
	 * 
	 * 
	 * @param triplestore
	 * @param confidenceLevel
	 * @param minLikelihood
	 * @throws IOException
	 * @throws InterruptedException 
	 * 
	 * 
	 * @author corentinviemon
	 */
	public C3M_Algorithm(Triplestore triplestore, double confidenceLevel, double minLikelihood) throws IOException, InterruptedException {
		System.out.println("******** Parameters ********");
		
		//We retrieve all contexts of the first level
		this.K = new Data_Retrieve(triplestore, confidenceLevel, minLikelihood);
		this.graph = K.graph;
		this.delta = K.delta;
		this.triplestore = K.triplestore;
		this.minLikelihood = minLikelihood;
		this.likelihood = minLikelihood;
		this.minOccurrence = (int) K.minOccurrence;
		//YOU HAVE TO CHOOSE THE OUTPUT PATH
		this.fileOut = "test//Results(" + String.valueOf(minLikelihood) + ").csv";
		//this.fileOut = "/Users/corentinviemon/Desktop/Résultats/Résultats(" + String.valueOf(minLikelihood) + ").csv";
		System.out.println("Enf of data download in" + K.time() + "\r\n");
		
		//Start of the algorithm
		C3M_Main(K, minLikelihood, confidenceLevel);
		
		//Writing in the output file
		write(constraintsListToPrint);
	}
	
	
	
	
	public void C3M_Main(Data_Retrieve K, double minLikelihood, double confidenceLevel) throws InterruptedException {
		//It browses every relation in the relations List
		for (int i = 0 ; i < K.relationsList.size() ; i++) {
			System.out.println("\r\nRelation n°" + (i+1) + "/" + K.relationsList.size() + " [" + K.relationsList.get(i) + "]");
			//The relation rdf-type is skiped because of probleme of timeout and precision
			if (K.relationsList.get(i).equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
				System.out.println("--> Untreated relation");
				continue;
			}
			
			Map <String,CardinalityConstraint> tmpConstraintsList = new HashMap<String, CardinalityConstraint>();
			Map <String,CardinalityConstraint> tmpMinimumConstraintsList = new HashMap<String, CardinalityConstraint>();
			String context = "Top";
			String relation = K.relationsList.get(i);
			int cardinality = Integer.MAX_VALUE;
			
			//We retrieve all contexts of the first level for the given relation
			K.levelExploration(relation);
			//We start the algorithm C3M_Explore with the given relation and with the context TOP
			tmpConstraintsList = C3M_Explore(K, relation, context, cardinality, minLikelihood, confidenceLevel);
			
			//Then, when we have all constraints for the given relation, we retrieve all minimum maximum cardinalities
			tmpMinimumConstraintsList = computeMinimumMaximumCardinality(tmpConstraintsList);

			System.out.println("Time elapsed since the beginning:" + K.time());
	
			//Every constraints retrieved are put in the final constraints Hashmap and it also permits to avoid duplicate
			for (Map.Entry <String, CardinalityConstraint> mapentry : tmpMinimumConstraintsList.entrySet()) {
				String key = mapentry.getValue().relation + ";" + mapentry.getValue().context;
				constraintsListToPrint.putIfAbsent(key, mapentry.getValue());
				//System.out.println("Context : " + mapentry.getValue().context+ " MaximumCardinality : " +mapentry.getValue().maximumCardinality + " Likelihood : " + mapentry.getValue().likelihood+ " Level : " + mapentry.getValue().level);
			}
			System.out.println(constraintsListToPrint.size());
			//the relation will change so different ArrayList are cleared because context will be different
			contextsDone.clear();
			tempConstraintsList.clear();
		}
	}
	
	
	
	
	
	
	
	public Map <String,CardinalityConstraint> C3M_Explore(Data_Retrieve K, String relation, String context, int cardinality, double minLikelihood, double confidenceLevel) throws InterruptedException {
		if (context.equals("Top")){
			//Query to retrieve cardinalities in the case of the TOP Level
			String queryStr = Triplestore.getPrefix()
					+ "SELECT ?cardinality (COUNT(?entity) AS ?count) FROM " + graph + " WHERE {\r\n" + 
					"SELECT ?entity (COUNT(?relation) AS ?cardinality) WHERE { ?entity <" + relation + "> ?relation} GROUP BY ?entity\r\n" + 
					"}\r\n" + 
					"GROUP BY ?cardinality\r\n"+
					"ORDER BY DESC(?cardinality)";
			
			CardinalityQuery query = new CardinalityQuery(queryStr, triplestore.getEndpoint());
			query.execute();
			//To manage error and retry the query after a sleep time
			while (query.error == true) {
				TimeUnit.SECONDS.sleep(20);
				System.out.println("ERROR! relation: " + relation + "  context: " + context);
				query.execute();
			}

			//Compute the maximum cardinality
			CardinalityConstraint constraint = computeMaximumCardinality(query.countPerCardinalities);
			if(! query.countPerCardinalities.isEmpty() && constraint.getLikelihood() >= minLikelihood) {
				constraint.setContext("Top");
				constraint.setRelation(relation);
				constraint.setLevel(0);
				String key = relation + ";" + context;
				tempConstraintsList.putIfAbsent(key, constraint);
			}
			
			//If the maximum cardinality is greater than 1, the HashMap taht contains first level is browsed
			if(constraint.getMaximumCardinality() > 1){
				System.out.println("Level 1 -> " + K.levelExplore.get(1).size() + " contexts for the given relation");
				for(int numContext = 0; numContext < K.levelExplore.get(1).size() ; numContext++) {
					level = 1;
					context = K.levelExplore.get(1).get(numContext);
					if(numContext%100 == 0) //We priting a message every 100 relations
						System.out.println("\tLevel 1 : context "+ (numContext+1) + "/" + K.levelExplore.get(1).size() + " [Context : " + context + "]");
					if(! contextsDone.contains(context)) { //Test to be sure that this context has not been done yet
						contextsDone.add(context); //Context is added to an arraylist to be sure that the cardinality for this context and for the given relation is not computed twice
						C3M_Explore(K, relation, context, cardinality, minLikelihood, confidenceLevel);
					}
				}
			}
		}

		//If the context is not the context TOP
		else {
			getHierarchies(context);
			//This is the query to retrieve cardinalities for a given context and a given relation
			String queryStr = Triplestore.getPrefix()
					+ "SELECT ?cardinality (COUNT(?entity) AS ?count) FROM " + graph + " WHERE {\r\n" + 
					"SELECT ?entity (COUNT(?relation) AS ?cardinality) WHERE {?entity a <" + context + ">. ?entity <" + relation + "> ?relation} GROUP BY ?entity\r\n" + 
					"}\r\n" + 
					"GROUP BY ?cardinality\r\n" + 
					"ORDER BY DESC(?cardinality)";
			
			CardinalityQuery query = new CardinalityQuery(queryStr, triplestore.getEndpoint());
			query.execute();
			//To manage error and retry the query after a sleep time
			while (query.error == true) {
				TimeUnit.SECONDS.sleep(20);
				System.out.println("ERROR! relation: " + relation + "  context: " + context);
				query.execute();
			}
			
			//We compute the maximum cardinality
			CardinalityConstraint constraint = computeMaximumCardinality(query.countPerCardinalities);
			//If there is a maximum cardinality and if the likelihood is greater than the likelihood threshold
			if(! query.countPerCardinalities.isEmpty() && constraint.getLikelihood() >= minLikelihood) {
				constraint.setContext(context);
				constraint.setRelation(relation);
				constraint.setLevel(level);
				String key = relation + ";" + context;
				tempConstraintsList.putIfAbsent(key, constraint);
			}
			//If the maximum cardinality computed is greater than 1, we retrieve direct sub-contexts
			if((constraint.getMaximumCardinality() > 1)) {
				queryStr = Triplestore.getPrefix()
						+ "SELECT distinct ?subContext FROM " + graph + " WHERE {\r\n"
						+ "?subContext rdfs:subClassOf <"+ context + ">. ?x a ?subContext.}\r\n"
						+ "GROUP BY ?subContext HAVING (COUNT(?x)>= " + minOccurrence + ")";
				
				/*Triplestore.getPrefix()
						+ "SELECT distinct ?subContext FROM " + graph + " WHERE {\r\n"
						+ "?subContext rdfs:subClassOf <"+ context + ">. ?x a ?subContext. ?x <" + relation + "> ?obj}\r\n"
						+ "GROUP BY ?subContext HAVING (COUNT(?x)>= " + minOccurrence + ") ORDER BY DESC(COUNT(?x))";*/
				
				
				ContextsRelationsQuery contextsQuery = new ContextsRelationsQuery(queryStr, triplestore.getEndpoint(), "subContext");
				contextsQuery.execute();
				//To manage error and retry the query after a sleep time
				while (contextsQuery.error == true) {
					TimeUnit.SECONDS.sleep(20);
					System.out.println("ERROR! relation: " + relation + "  context: " + context);
					contextsQuery.execute();
				}

				//If there are sub-contexts, the arraylist is browsed
				if (! contextsQuery.resultsList.isEmpty()) {
					level ++;
					for (int k=0; k<contextsQuery.resultsList.size(); k++) {
						context = contextsQuery.resultsList.get(k);
						if(! contextsDone.contains(context)) { //Test to be sure that this context has not been done yet
							contextsDone.add(context); //Context is added to an arraylist to be sure that the cardinality for this context and for the given relation is not computed twice
							C3M_Explore(K, relation, context, cardinality, minLikelihood, confidenceLevel);
						}
						else if(tempConstraintsList.containsKey(relation+ ";" +context) && tempConstraintsList.get(relation+ ";" +context).level>level) {
							tempConstraintsList.get(relation+ ";" +context).setLevel(level);
							System.out.println("Relation: " + relation + "  Context: " + context + " -> Shortest path for the level found");
						}
					}
					level --;
				}
			}	
		}
		return tempConstraintsList;
	}
	
	
	
	
	/**
	 * This method permits to compute the maximum cardinality according to 
	 * an ArrayList of cardinalities and their number of occurrences
	 * 
	 * 
	 * @param countPerCardinalities
	 * @return
	 */
	public CardinalityConstraint computeMaximumCardinality(ArrayList <CountPerCardinality> countPerCardinalities) {
		CardinalityConstraint cardConstraint = new CardinalityConstraint();
		double underMinLower = 0;
		likelihood = minLikelihood;
		maximumCardinality = Integer.MAX_VALUE;
		int n = 0;
		//the ArrayList is browsed to retrieve each cardinality and its number of occurrences
		for (int i = 0; i < countPerCardinalities.size(); i++) {
			int cardinality = countPerCardinalities.get(i).cardinality;
			int count = countPerCardinalities.get(i).count;
			n += count;
			//Likelihhod is computed according to the Hoeffding's inequality
			double avg = ((double)count) / n;
			double error = Math.sqrt(Math.log(1 / delta) / (2 * n));
			double lower = Math.max(avg - error, 0);
			//If the computed likelihood is greater than the threshold
			if (lower > likelihood) {
				maximumCardinality = cardinality; //the maximum cardinality value is replaced
				likelihood = lower; //and the threshold is replaced by the likelihood that is higher
			}
			else{
				if (lower > underMinLower) //Else the the highest likelihood under the threshold is retrieved
					underMinLower = lower;
			}
		}
		if (likelihood == minLikelihood){ //If the likelihood equals to the threshold (if there is no maximum cardinality detected)
			cardConstraint.likelihood = underMinLower; //the likelihood is the highest value under the threshold
		}
		else {
			cardConstraint.likelihood = likelihood; //else, the likelihhod is the highest value retrieved
		}
		cardConstraint.maximumCardinality = maximumCardinality;
		return cardConstraint;
	}
	
	
	
	/**
	 * This methods permits to compute minimum maximum cardinalities according to an
	 * arrayList. It browses the arrayList and searches for a given context, if there is a top context
	 * with the same relation and the same cardinality.
	 * 
	 * 
	 * @param inputConstraintsList
	 * @return
	 */
	public Map <String, CardinalityConstraint> computeMinimumMaximumCardinality(Map <String, CardinalityConstraint> inputConstraintsList) {

		for (Map.Entry <String, CardinalityConstraint> mapentry : inputConstraintsList.entrySet()) {
			System.out.println("Context : " + mapentry.getValue().context+ " MaximumCardinality : " +mapentry.getValue().maximumCardinality + " Likelihood : " + mapentry.getValue().likelihood+ " Level : " + mapentry.getValue().level);
		}
		
		
		
		Map <String, CardinalityConstraint> list = new HashMap<String, CardinalityConstraint>();
		int cardinalityTop = Integer.MAX_VALUE;
		
		for (Map.Entry <String, CardinalityConstraint> mapentryTop : inputConstraintsList.entrySet()) {
			if(mapentryTop.getValue().context.equals("Top")) {
				cardinalityTop = mapentryTop.getValue().maximumCardinality;
				String key = mapentryTop.getValue().relation + ";" + mapentryTop.getValue().context;
				list.putIfAbsent(key, mapentryTop.getValue());
			}
			else
				continue;
		}
		
		for (Map.Entry <String, CardinalityConstraint> mapentry : inputConstraintsList.entrySet()) {
			CardinalityConstraint topCardConstr = new CardinalityConstraint();
			topCardConstr.setContext(mapentry.getValue().context);
			topCardConstr.setRelation(mapentry.getValue().relation);
			topCardConstr.setLevel(mapentry.getValue().level);
			topCardConstr.setMaximumCardinality(mapentry.getValue().maximumCardinality);
			topCardConstr.setLikelihood(mapentry.getValue().likelihood);
			ArrayList <String> listMeres = new ArrayList<String>();
		
			if (topCardConstr.getContext().equals("Top") || topCardConstr.getMaximumCardinality() == cardinalityTop)
				continue;
			
			if(hierarchies.containsKey(topCardConstr.getContext()))
				listMeres = hierarchies.get(topCardConstr.getContext());
			else if (mapentry.getValue().maximumCardinality < cardinalityTop) {
				String key = topCardConstr.getRelation() + ";" + topCardConstr.getContext();
				list.putIfAbsent(key, mapentry.getValue());
				continue;
			}
			
			if(! listMeres.isEmpty()){
				for (int nb=0 ; nb<listMeres.size(); nb++){
					String mere = topCardConstr.getRelation() +";"+listMeres.get(nb);
					if (inputConstraintsList.containsKey(mere)) {
						if(inputConstraintsList.get(mere).maximumCardinality == topCardConstr.getMaximumCardinality() && inputConstraintsList.get(mere).level < topCardConstr.getLevel()) {
							topCardConstr.setContext(inputConstraintsList.get(mere).context);
							topCardConstr.setLevel(inputConstraintsList.get(mere).level);
							topCardConstr.setLikelihood(inputConstraintsList.get(mere).likelihood);
							topCardConstr.setMaximumCardinality(inputConstraintsList.get(mere).maximumCardinality);
						}
					}
					else
						continue;
				}
			}
			if(topCardConstr.getMaximumCardinality() < cardinalityTop) {
				String key = topCardConstr.relation + ";" + topCardConstr.getContext();
				list.putIfAbsent(key, topCardConstr);
			}
			else {
				continue;
			}
		}
		
		return list;
	}	
	
	
	
	
	/**
	 * This method permits to retrieve all top contexts for a given context. Results are stocked
	 * in a Hashmap <context, ArrayList of top Contexts> to be used during all the execution
	 * 
	 * @param contexteFille
	 * @throws InterruptedException
	 */
	public void getHierarchies(String context) throws InterruptedException {
		String queryStr = Triplestore.getPrefix()
				+ "SELECT ?mere FROM " + graph + " WHERE {<" + context + "> rdfs:subClassOf+ ?mere. "
				+ "?entity a ?mere"
				+ "}"
				+ "GROUP BY ?mere\r\n"
				+ "ORDER BY ASC (count(?entity))";
		ContextsRelationsQuery query = new ContextsRelationsQuery(queryStr, triplestore.getEndpoint(), "mere");
		query.execute();
		//To manage error and retry the query after a sleep time
		while (query.error == true) {
			TimeUnit.SECONDS.sleep(20);
			System.out.println("ERROR!  context: " + context);
			query.execute();
		}
		
		if(! query.resultsList.isEmpty())
			hierarchies.putIfAbsent(context, query.resultsList); //Une fois toutes les relations mères récupérées, on les mets dans la HashMap avec la relation fille en clé
	}
	
	
	
	/**
	 * This method permits to write every contraint in the output file.
	 * 
	 * @param constraintsList
	 * @throws IOException
	 */
	public void write(Map <String, CardinalityConstraint> constraintsList) throws IOException {
		File ff = new File(fileOut);
		ff.createNewFile();
		ffw = new FileWriter(ff);
		ffw.write("Context;Relation;Cardinality;Likelihood;Level\r\n"); //Header du fichier csv
		for (Map.Entry <String, CardinalityConstraint> mapentry : constraintsList.entrySet()) {
			String relation = mapentry.getValue().relation;
			String context = mapentry.getValue().context;
			String cardinality = String.valueOf(mapentry.getValue().maximumCardinality);
			String likelihood = String.valueOf(mapentry.getValue().likelihood);
			String level = String.valueOf(mapentry.getValue().level);
			
			//Just a test to do not write level 0 for TOP level in the output
			if(mapentry.getValue().level == 0)
				level = "";
			
			ffw.write(context + ";" + relation + ";" + cardinality + ";" + likelihood + ";" + level + "\r\n");
			ffw.flush();
		}
		ffw.close();
		System.out.println("\r\n------End. Execution Time: "+ K.time() + "------");
	}
	
	
	
	
	
	
	
	
	
	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		C3M_Algorithm Algorithm = new C3M_Algorithm(Triplestore.DBPEDIA, 0.99, 0.97);
	}

}
