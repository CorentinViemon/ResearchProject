package C3M;

/**
 * <b>Classe énumérant les différents endpoints pour chacun des LOD</b>
 * <p> 5 Triplestores sont renseignés ici </p>
 *
 * 
 * @author M. Soulet
 * 
 */
public enum Triplestore {
	DBPEDIA("http://dbpedia.org/sparql"),
	WIKIDATA("https://query.wikidata.org/sparql"),
	CIDOC_CRM("http://vega.info.univ-tours.fr/blazegraph/16348/sparql"),
	YAGO("https://linkeddata1.calcul.u-psud.fr/sparql"),
	CHEMBL("https://www.ebi.ac.uk/rdf/services/sparql");
	
	private String endpoint;

	/**
	 * Constructeur
	 * @param endpoint
	 * 				L'endpoint du Triplestore
	 */
	private Triplestore(String endpoint) {
		this.endpoint = endpoint;
	}
	
	/**
	 * 
	 * @return l'endpoint du Triplestore
	 */
	public String getEndpoint() {
		return endpoint;
	}
	
	/**
	 * Retourne les préfixes communs aux différents Triplestore
	 * 
	 * @return l'ensemble des préfixes
	 */
	public static String getPrefix() {
		return "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dbr: <http://dbpedia.org/resource/> PREFIX wd: <http://www.wikidata.org/entity/> prefix dbo: <http://dbpedia.org/ontology/> prefix wdt: <http://www.wikidata.org/prop/direct/> ";
	}
}
