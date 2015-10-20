package db.schema.entity;

import db.schema.utils.AttributeUtils;
import db.schema.utils.WorkloadUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

/**
 * A class describing a database table and all queries touching that table,
 * together forming what we call a workload.
 *
 * @author Endre Palatinus
 */
public class Workload {

    public String tableName;
    public List<Attribute> attributes;
    /** All queries touching the table, but containing information only relevant to scanning and filtering this table. */
    public List<Query> queries;
    private List<Range> rangeFilterConditions;
    private long numRows;
    public String dataFileName = null;

    /** A plan for each query regarding which partitions to use. */
    private TIntObjectHashMap<TIntHashSet> bestSolutions;

    public Workload(List<Attribute> attributes, long numRows, String tableName) {
        this.tableName = tableName;
        this.attributes = attributes;
        this.numRows = numRows;
        queries = new ArrayList<>();
        rangeFilterConditions = new ArrayList<>();
    }

    /**
     * Add a list of queries to the workload, that are all project-only queries.
     * @param queries the list of queries
     */
    public void addProjectionQueries(List<Query> queries) {
        this.queries.addAll(queries);
    }

    /**
     * Add a project-only query to the workload.
     * @param name the name of the query
     * @param weight the relative occurrence or importance of the query within the workload
     * @param projections the set of attributes projected by the query
     */
    public void addProjectionQuery(String name, int weight, int... projections) {
        Query q = new Query(name, weight);
        q.setProjections(attributes.size(), projections);
        q.setSelectivity(1.0);
        queries.add(q);
    }

    /**
     * Method for adding a query with projected columns and selections as well to the workload.
     */
    public void addProjectionQueryWithFiltering(String name, int weight, int[] filteredColumns, double selectivity, int... projections) {
        Query q = new Query(name, weight);
        q.setProjections(attributes.size(), projections);
        q.setSelectivity(selectivity);
        q.setFilteredColumns(filteredColumns);
        queries.add(q);
    }

    public void addRangeQuery(String name, int weight, Range selection, int... projections) {
        Query q = new RangeQuery(name, weight, selection);
        q.setProjections(attributes.size(), projections);
        queries.add(q);
        rangeFilterConditions.add(selection);
    }

    /**
     * Method for creating a matrix representation of the workload, showing which query projects which attribute.
     * Attributes only used in filter conditions but not projected are not included in this matrix.
     * @return a |queries| x |attributes| matrix of 1's and 0's
     */
    public int[][] getUsageMatrix() {
        int[][] usageM = new int[queries.size()][0];
        for (int i = 0; i < usageM.length; i++) {
            usageM[i] = queries.get(i).getAttributeUsageVector();
        }
        return usageM;
    }

    public Range[] getRangeFilterConditions() {
        return (Range[])rangeFilterConditions.toArray();
    }

    public long getNumberOfRows() {
        return this.numRows;
    }

    public int[] getQueryWeights() {
        return WorkloadUtils.getQueryWeights(this);
    }

    public int[] getAttributeSizes() {
        return AttributeUtils.getAttributeSizes(attributes);
    }

    public TIntObjectHashMap<TIntHashSet> getBestSolutions() {
        return bestSolutions;
    }

    public void setBestSolutions(TIntObjectHashMap<TIntHashSet> bestSolutions) {
        this.bestSolutions = bestSolutions;
    }

    /**
     * Method for finding a query by its name.
     * @param name The name of the query.
     * @return The query object.
     */
    public Query getQueryByName(String name) {

        for (Query q : queries) {
            if (q.name.equals(name)) {
                return q;
            }
        }

        return null;
    }

    /**
     * Efficient representation of the Workload with primitive typed attributes.
     * <p>
     * Note that any changes in the Workload instance are not cascaded in the SimplifiedWorkload instances
     * created before the changes.
     */
    public class SimplifiedWorkload implements Cloneable {

        /**
         * The a matrix representation of the workload, showing which query projects which attribute:
         * |queries| x |attributes| matrix of 1's and 0's
         * Attributes only used in filter conditions but not projected are not included in this matrix.
         */
        public int[][] usageMatrix;
        /** The sizes of the attributes in bytes. */
        public int[] attributeSizes;
        public int attributeCount;
        public int queryCount;
        /** The relative occurrence or importance of the queries within the workload. */
        public int[] queryWeights; // TODO use this in some algorithms
        public long numRows;
        /** The total size of a single database row. */
        public int rowSize;
        /**
         * The a matrix representation of the workload, showing which query filters which attribute:
         * |queries| x |attributes| matrix of 1's and 0's
         * Attributes only used in projections but not filtered are not included in this matrix.
         */
        public int[][] selectivityColumns;
        /** The selectivity of each query. */
        public double[] selectivities;

        /**
         * Constructor that creates an instance using the data of the enclosing Workload instance.
         */
        protected SimplifiedWorkload() {

            usageMatrix = getUsageMatrix();
            attributeSizes = getAttributeSizes();
            queryCount = usageMatrix.length;

            try {
                attributeCount = usageMatrix[0].length;
            } catch (Exception ex) {
                attributeCount = 0;
            }
            queryWeights = getQueryWeights();
            numRows = getNumberOfRows();

            rowSize = 0;
            for (int a = 0; a < attributeCount; a++) {
                rowSize += attributeSizes[a];
            }

            selectivityColumns = new int[queryCount][];
            selectivities = new double[queryCount];

            for (int q = 0; q < queryCount; q++) {
                selectivityColumns[q] = queries.get(q).getFilteredColumns() == null
                        ? new int[0]
                        : queries.get(q).getFilteredColumns().clone();
                selectivities[q] = queries.get(q).getSelectivity();
            }
        }

        @Override
        public Object clone() {
            return new SimplifiedWorkload();
        }
    }

    public SimplifiedWorkload getSimplifiedWorkload() {
        return new SimplifiedWorkload();
    }

    /**
     * Create a simple SELECT ... FROM ... query string for a given query.
     *
     * @param queryName The name of the query.
     * @return The leaf-level query string.
     */
    public String getLeafLevelQueryString(String queryName) {
        StringBuilder sb = new StringBuilder();

        if (getQueryByName(queryName) == null) {
            return "";
        }

        int[] projectedAttrs = getQueryByName(queryName).getProjectedColumns();

        sb.append("SELECT ");
        for (int a_i = 0; a_i < projectedAttrs.length; a_i++) {
            int a = projectedAttrs[a_i];
            sb.append(attributes.get(a).name);

            if (a_i < projectedAttrs.length - 1) {
                sb.append(", ");
            } else {
                sb.append("\nFROM ").append(tableName).append(';');
            }
        }

        return sb.toString();
    }
}
