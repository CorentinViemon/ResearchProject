package C3M;


/**
 * Class to define characteristics of a cardinality constraint
 * A constraint has :
 * <ul>
 * <li>A context</li>
 * <li>A relation</li>
 * <li>A maximum Cardinality</li>
 * <li>A level</li>
 * <li>A likelihood</li>
 * </ul>
 * @author corentinviemon
 *
 */
public class CardinalityConstraint {

	public String context;
	public String relation;
	public int maximumCardinality;
	public int level;
	public double likelihood;
	
	
	/**
	 * Constructor
	 * @param context
	 * @param relation
	 * @param maximumCardinality
	 * @param likelihood
	 * @param level
	 */
	public CardinalityConstraint(String context, String relation, int maximumCardinality, double likelihood, int level) {
		this.context = context;
		this.relation = relation;
		this.maximumCardinality = maximumCardinality;
		this.likelihood = likelihood;
		this.level = level;
	}
	
	
	/**
	 * Constructor
	 */
	public CardinalityConstraint() {
		
	}


	public int getLevel() {
		return level;
	}


	public void setLevel(int level) {
		this.level = level;
	}


	public String getContext() {
		return context;
	}


	public void setContext(String context) {
		this.context = context;
	}


	public String getRelation() {
		return relation;
	}


	public void setRelation(String relation) {
		this.relation = relation;
	}


	public int getMaximumCardinality() {
		return maximumCardinality;
	}


	public void setMaximumCardinality(int maximumCardinality) {
		this.maximumCardinality = maximumCardinality;
	}


	public double getLikelihood() {
		return likelihood;
	}


	public void setLikelihood(double likelihood) {
		this.likelihood = likelihood;
	}	
	
	
	
	
}
