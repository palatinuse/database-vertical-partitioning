package db.schema.entity;

import db.schema.utils.QueryUtils;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A query projecting some columns, and optionally performing filtering (selections) as well.
 * It contains information only relevant to scanning and filtering a single table.
 * Database queries touching multiple tables have to be represented by a separate Query object for each table being touched.
 *
 * @author Endre Palatinus
 *
 */
public class Query {

    /** The name of the query. */
    protected String name;
    /** The relative occurrence or importance of the query within the workload. */
    protected int weight;
    /** The number of attributes of the table touched by this query. */
    protected int numberOfAttributesOfTable;
    /** The list of attributes projected by the query. */
    protected int[] projectedColumns;
    /** Columns being filtered, i.e. appearing in a selection operator. */
    protected int[] filteredColumns;
    /** The selectivity of the query. */
    protected double selectivity;
	/** A plan for the query regarding which partitions to use. */
    protected TIntHashSet bestSolution;   // TODO set this attribute from the Workload in case of use!!!

    /**
     * Constructor taking only the name and weight of the query.
     * Projections and selections have to be added separately.
     * @param name the name of the query
     * @param weight the relative improtance of the query within the workload
     */
	public Query(String name, int weight){
		this.name = name;
		this.weight = weight;
	}

    /**
     * A copy constructor creating a deep copy of its parameter.
     * @param query the query object to be cloned
     */
    public Query(Query query) {
        this.name = query.name;
        this.weight = query.weight;
        this.numberOfAttributesOfTable = query.numberOfAttributesOfTable;
        this.projectedColumns = query.projectedColumns == null ? null : query.projectedColumns.clone();
        this.filteredColumns = query.filteredColumns == null ? null : query.filteredColumns.clone();
        this.selectivity = query.selectivity;
        this.bestSolution = query.bestSolution == null ? null : new TIntHashSet(query.bestSolution);
    }

    public String toString(Table t){
        String query = "SELECT " + QueryUtils.getProjectionString(t, projectedColumns)
                + " FROM " + QueryUtils.getSourceTableString(t, projectedColumns, bestSolution);
        String joinString = QueryUtils.getJoinConditionString(t, projectedColumns, bestSolution);

        if (joinString!=null && !joinString.isEmpty()) {
            query += " WHERE " + joinString;
        }

        return query + ";";
    }

    /**
     * Method for creating a bitmask of the attributes projected by the query.
     * Attributes only filtered but not projected are not included in the usage vector.
     * @return the bitmask showing which attributes are projected
     */
	public int[] getAttributeUsageVector(){
		int[] usage = new int[numberOfAttributesOfTable];
		for(int p: projectedColumns)
			usage[p] = 1;
		return usage;
	}

	public int[] getProjectedColumns(){
		return this.projectedColumns;
	}

    public void setProjections(int numAttributes, int... projections){
        this.numberOfAttributesOfTable = numAttributes;
        this.projectedColumns = projections;
    }

	public int getNumberOfAttributesOfTable(){
		return this.numberOfAttributesOfTable;
	}

	public TIntHashSet getBestSolution() {
		return bestSolution;
	}

	public void setBestSolution(TIntHashSet bestSolution) {
		this.bestSolution = bestSolution;
	}

    public int[] getFilteredColumns() {
        return filteredColumns;
    }

    public void setFilteredColumns(int[] filteredColumns) {
        this.filteredColumns = filteredColumns;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public void setSelectivity(double selectivity) {
        this.selectivity = selectivity;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }
}