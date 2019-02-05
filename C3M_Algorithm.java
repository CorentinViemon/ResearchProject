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
		this.K = new Data_Retrieve(Triplestore.DBPEDIA, confidenceLevel, minLikelihood);
		this.graph = K.graph;
		this.delta = K.delta;
		this.triplestore = K.triplestore;
		this.minLikelihood = minLikelihood;
		this.likelihood = minLikelihood;
		this.minOccurrence = (int) K.minOccurrence;
		//YOU HAVE TO CHOOSE THE OUTPUT PATH
		this.fileOut = "test//Results(" + String.valueOf(minLikelihood) + ").csv";
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
			
			//Then, when we have all constraints for the given relation, we retrieve all minim maximum cardinalities
			tmpMinimumConstraintsList = computeMinimumMaximumCardinality(tmpConstraintsList);

			System.out.println("Time elapsed since the beginning:" + K.time());
	
			//Every constraints retrieved are put in the final constraints Hashmap and it also permits to avoid duplicate
			for (Map.Entry <String, CardinalityConstraint> mapentry : tmpMinimumConstraintsList.entrySet()) {
				String key = mapentry.getValue().relation + ";" + mapentry.getValue().context;
				constraintsListToPrint.putIfAbsent(key, mapentry.getValue());
				System.out.println("Context : " + mapentry.getValue().context+ " MaximumCardinality : " +mapentry.getValue().maximumCardinality + " Likelihood : " + mapentry.getValue().likelihood+ " Level : " + mapentry.getValue().level);
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
				TimeUnit.SECONDS.sleep(15);
				query.execute();
			}

			//Compute the maximum cardinality
			CardinalityConstraint constraint = computeMaximumCardinality(query.countPerCardinalities);
			if(! query.countPerCardinalities.isEmpty() && constraint.likelihood >= minLikelihood) {
				constraint.context = "Top";
				constraint.relation = relation;
				constraint.level = 0;
				String key = relation + ";" + context;
				tempConstraintsList.putIfAbsent(key, constraint);
			}
			
			//If the maximum cardinality is greater than 1, the HashMap taht contains first level is browsed
			if(constraint.maximumCardinality > 1){
				System.out.println("Level 1 -> " + K.levelExplore.get(1).size() + " classes pour la relation donnée");
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
				TimeUnit.SECONDS.sleep(15);
				query.execute();
			}
			
			//We compute the maximum cardinality
			CardinalityConstraint constraint = computeMaximumCardinality(query.countPerCardinalities);
			//If there is a maximum cardinality and if the likelihood is greater than the likelihood threshold
			if(! query.countPerCardinalities.isEmpty() && constraint.likelihood >= minLikelihood) {
				constraint.context = context;
				constraint.relation = relation;
				constraint.level = level;
				String key = relation + ";" + context;
				tempConstraintsList.putIfAbsent(key, constraint);
			}
			//If the maximum cardinality computed is greater than 1 and if it's not an error (if the cardinality is infinite), we retrieve direct sub-contexts
			if((! query.countPerCardinalities.isEmpty() && constraint.maximumCardinality > 1) || (query.countPerCardinalities.isEmpty() && query.error == true)) {
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
					TimeUnit.SECONDS.sleep(15);
					contextsQuery.execute();
				}

				//If there is sub-contexts, the arrylist is browsed
				if (! contextsQuery.relationsList.isEmpty()) {
					level ++;
					for (int k=0; k<contextsQuery.relationsList.size(); k++) {
						context = contextsQuery.relationsList.get(k);
						if(! contextsDone.contains(context)) { //Test to be sure that this context has not been done yet
							contextsDone.add(context); //Context is added to an arraylist to be sure that the cardinality for this context and for the given relation is not computed twice
							C3M_Explore(K, relation, context, cardinality, minLikelihood, confidenceLevel);
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
		
		Map <String, CardinalityConstraint> list = new HashMap<String, CardinalityConstraint>();
		ArrayList <String> tmpContexts = new ArrayList <String>();
		ArrayList <CardinalityConstraint> constraintsList = new ArrayList <CardinalityConstraint>();
		ArrayList <CardinalityConstraint> fixConstraintsList = new ArrayList <CardinalityConstraint>();
		int cardinalityTop = Integer.MAX_VALUE;
		int level = 0;
		
		for (Map.Entry <String, CardinalityConstraint> mapentryTop : inputConstraintsList.entrySet()) {
			if(mapentryTop.getValue().context.equals("Top")) {
				cardinalityTop = mapentryTop.getValue().maximumCardinality;
				fixConstraintsList.add(mapentryTop.getValue());
			}
			else
				continue;
		}
		
		for (Map.Entry <String, CardinalityConstraint> mapentry : inputConstraintsList.entrySet()) {
			String relation = mapentry.getValue().relation; 
			String context = mapentry.getValue().context;
			int cardinality = mapentry.getValue().maximumCardinality;
			level = mapentry.getValue().level;
			ArrayList <String> listMeres = new ArrayList<String>();
			
			if (context.equals("Top") || cardinality == cardinalityTop || tmpContexts.contains(relation + ";" + context))
				continue;
			
			if(hierarchies.containsKey(context) && ! hierarchies.get(context).isEmpty())
				listMeres = hierarchies.get(context);
			else if (mapentry.getValue().maximumCardinality < cardinalityTop) {
				fixConstraintsList.add(mapentry.getValue());
			}
			
			if(! listMeres.isEmpty()){
				boolean hasTopClass = false;
				boolean multiple = false;
				for (int nb=0 ; nb<listMeres.size(); nb++){
					String mere = relation +";"+listMeres.get(nb);
					if (inputConstraintsList.containsKey(mere)) {
						if((inputConstraintsList.get(mere).maximumCardinality == cardinality || multiple == true) && inputConstraintsList.get(mere).maximumCardinality < level) {
							hasTopClass = true;
							level = inputConstraintsList.get(mere).level;
							int fixSize = constraintsList.size();
							int changingSize = fixSize;
							if(! constraintsList.isEmpty()) {
								int i = 0;
								while(i < changingSize) {
									if(constraintsList.get(i).maximumCardinality == inputConstraintsList.get(mere).maximumCardinality) {
										constraintsList.remove(i);
										changingSize = constraintsList.size();
									}
									else 
										i++;
									multiple = false;
								}
							}
							if(constraintsList.size() < fixSize || constraintsList.isEmpty()) {
								constraintsList.add(inputConstraintsList.get(mere));
								cardinality = inputConstraintsList.get(mere).maximumCardinality;
							}
						}
						else if(inputConstraintsList.get(mere).level == level && inputConstraintsList.get(mere).maximumCardinality < cardinalityTop) {
							hasTopClass = true;
							multiple = true;
							if(inputConstraintsList.get(mere).maximumCardinality < cardinality)
								cardinality = inputConstraintsList.get(mere).maximumCardinality;
							constraintsList.add(inputConstraintsList.get(mere));
						}
						tmpContexts.add(mere);
					}
				}
				if(hasTopClass == false && mapentry.getValue().maximumCardinality < cardinalityTop) {
					fixConstraintsList.add(mapentry.getValue());
				}	
				
				else {
					for(int i = 0 ; i < constraintsList.size(); i++) {
						fixConstraintsList.add(constraintsList.get(i));
					}
					constraintsList.clear();
				}
			}
		}
		
		for(int k=0 ; k < fixConstraintsList.size(); k++) {
			String key = fixConstraintsList.get(k).relation + ";" + fixConstraintsList.get(k).context;
			list.putIfAbsent(key, fixConstraintsList.get(k));
		}
		
		
		
		
		/*int cardinality = cardinalityTop;
		for (Map.Entry <String, CardinalityConstraint> mapentry : inputConstraintsList.entrySet()) {
			String relation = mapentry.getValue().relation; 
			String context = mapentry.getValue().context;
			ArrayList <String> listMeres = new ArrayList<String>();
			if (context.equals("Top") || tmpContexts.contains(relation + ";" + context))
				continue;
			
			if(hierarchies.containsKey(context) && ! hierarchies.get(context).isEmpty())
				listMeres = hierarchies.get(context);
			else if (mapentry.getValue().maximumCardinality < cardinalityTop) {
				fixConstraintsList.add(mapentry.getValue());
			}
			if(! listMeres.isEmpty()){
				boolean hasTopClass = false;
				for (int nb=0 ; nb<listMeres.size(); nb++){
					String mere = relation +";"+listMeres.get(nb);
					if (inputConstraintsList.containsKey(mere) && ! tmpContexts.contains(mere)) {
						if(inputConstraintsList.get(mere).maximumCardinality < cardinality) {
							hasTopClass = true;
							level = inputConstraintsList.get(mere).level;
							cardinality = inputConstraintsList.get(mere).maximumCardinality;
							constraintsList.add(inputConstraintsList.get(mere));
						}
						else if(inputConstraintsList.get(mere).level == level && inputConstraintsList.get(mere).maximumCardinality < cardinalityTop) {
							hasTopClass = true;
							if(inputConstraintsList.get(mere).maximumCardinality < cardinality)
								cardinality = inputConstraintsList.get(mere).maximumCardinality;
							constraintsList.add(inputConstraintsList.get(mere));
						}
						tmpContexts.add(mere);
					}
				}
				if(hasTopClass == true) {
					for(int i = 0 ; i < constraintsList.size(); i++) {
						fixConstraintsList.add(constraintsList.get(i));
					}
					constraintsList.clear();
				}		
				else if (hasTopClass == false && mapentry.getValue().maximumCardinality < cardinalityTop)
					fixConstraintsList.add(mapentry.getValue());
			}
		}
		
		for(int k=0 ; k < constraintsList.size(); k++) {
			String key = constraintsList.get(k).relation + ";" + constraintsList.get(k).context;
			list.putIfAbsent(key, constraintsList.get(k));
		}
		for(int k=0 ; k < fixConstraintsList.size(); k++) {
			String key = fixConstraintsList.get(k).relation + ";" + fixConstraintsList.get(k).context;
			list.putIfAbsent(key, fixConstraintsList.get(k));
		}*/
		
		return list;
	}
	
	
	
	
	/**
	 * This method permits to retrieve all top contexts for a given context. Results are stocked
	 * in a Hashmap <context, ArrayList of top Contexts> to be used during all the execution
	 * 
	 * @param contexteFille
	 * @throws InterruptedException
	 */
	public void getHierarchies(String contexteFille) throws InterruptedException {
		String queryStr = Triplestore.getPrefix()
				+ "SELECT ?mere FROM " + graph + " WHERE {<" + contexteFille + "> rdfs:subClassOf+ ?mere. "
				+ "?entity a ?mere"
				+ "}"
				+ "GROUP BY ?mere\r\n"
				+ "ORDER BY ASC (count(?entity))";
		ContextsRelationsQuery query = new ContextsRelationsQuery(queryStr, triplestore.getEndpoint(), "mere");
		query.execute();
		//To manage error and retry the query after a sleep time
		while (query.error == true) {
			TimeUnit.SECONDS.sleep(15);
			query.execute();
		}
		
		if(! query.relationsList.isEmpty())
			hierarchies.putIfAbsent(contexteFille, query.relationsList); //Une fois toutes les relations mères récupérées, on les mets dans la HashMap avec la relation fille en clé
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
