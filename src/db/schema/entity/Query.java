package db.schema.entity;

import db.schema.utils.QueryUtils;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Map;


/**
 * A query that has projections, and optionally selections as well.
 * 
 * @author Alekh Jindal, Endre Palatinus
 *
 */
public class Query{
	
	public String name;
	public int weight;
	
	private int numAttributes;
	private int[] projections;
	
	private Range range;

    /** Columns that the selection operators reference. */
    private int[] selectivityColumns;
    /** The selectivity of the query */
    private double selectivity;

    // TODO set this variable from the Workload in case of use!!!
	/** A plan for the query regarding which partitions to use. */
	private TIntHashSet bestSolution;
	
	public Query(String name, int weight){
		this.name = name;
		this.weight = weight;
	}
	
	public void setProjections(int numAttributes, int... projections){
		this.numAttributes = numAttributes;
		this.projections = projections;
	}
	
	public void setSelection(Range range){
		this.range = range;
	}
	
//	public int[] getUsageArray(){
//		int[] usage = new int[numAttributes];
//		for(int p: projections)
//			usage[p] = 1;
//		return usage;
//	}
	
	public int[] getUsageArray(Map<Range,Integer> rangeIds){
		int rangeId = 1;
		// check if there is a selection predicate
		if(range!=null && rangeIds!=null && rangeIds.containsKey(range))
			rangeId = rangeIds.get(range);
		
		int[] usage = new int[numAttributes];
		for(int p: projections)
			usage[p] = rangeId;
		return usage;
	}
	
	public int[] getProjections(){
		return this.projections;
	}
	
	public Range getRange(){
		return this.range;
	}
	
	public int getNumAttributes(){
		return this.numAttributes;
	}
	
	public String toString(Table t){
		String query = "SELECT "+ QueryUtils.getProjectionString(t, projections) +" FROM "+ QueryUtils.getSourceTableString(t, projections, bestSolution);
		String joinString = QueryUtils.getJoinConditionString(t, projections, bestSolution);
		if(joinString!=null && joinString!="")
			query += " WHERE "+joinString;

		return query + ";";
	}

	public TIntHashSet getBestSolution() {
		return bestSolution;
	}

	public void setBestSolution(TIntHashSet bestSolution) {
		this.bestSolution = bestSolution;
	}

    public int[] getSelectivityColumns() {
        return selectivityColumns;
    }

    public void setSelectivityColumns(int[] selectivityColumns) {
        this.selectivityColumns = selectivityColumns;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }
}