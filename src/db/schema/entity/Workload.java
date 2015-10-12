package db.schema.entity;

import db.schema.utils.AttributeUtils;
import db.schema.utils.WorkloadUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Workload {

    public String tableName;
    public List<Query> queries;
    public List<Attribute> attributes;
    private long numRows;
    public String dataFileName = null;
    private Map<Range, Integer> rangeIds;
    private int maxRangeId = 0;
    /** A plan for each query regarding which partitions to use. */
    private TIntObjectHashMap<TIntHashSet> bestSolutions;

    public Workload(List<Attribute> attributes, long numRows, String tableName) {
        this.tableName = tableName;
        this.attributes = attributes;
        this.numRows = numRows;
        queries = new ArrayList<Query>();
        rangeIds = new HashMap<Range, Integer>();
    }

    public void addProjectionQueries(List<Query> queries) {
        this.queries.addAll(queries);
    }

    public void addProjectionQuery(String name, int weight, int... projections) {
        Query q = new Query(name, weight);
        q.setProjections(attributes.size(), projections);
        q.setSelectivity(1.0);
        q.setSelectivityColumns(new int[]{});
        queries.add(q);
    }

    /**
     * Method for adding a query with projections and selections to the workload.
     */
    public void addProjectionQuery(String name, int weight, int[] selectivityColumns, double selectivity, int... projections) {
        Query q = new Query(name, weight);
        q.setProjections(attributes.size(), projections);
        q.setSelectivity(selectivity);
        q.setSelectivityColumns(selectivityColumns);
        queries.add(q);
    }

    public void addSelectionQuery(String name, int weight, Range selection, int... projections) {
        Query q = new Query(name, weight);
        q.setProjections(attributes.size(), projections);
        q.setSelection(selection);
        queries.add(q);
        if (!rangeIds.containsKey(selection)) {
            rangeIds.put(selection, ++maxRangeId);
        }
    }

    public int[][] getUsageMatrix() {
        int[][] usageM = new int[queries.size()][0];
        for (int i = 0; i < usageM.length; i++) {
            usageM[i] = queries.get(i).getUsageArray(rangeIds);
        }
        return usageM;
    }

    public Range[] getWorkloadRanges() {
        Range[] ranges = new Range[rangeIds.size()];
        for (Range r : rangeIds.keySet()) {
            ranges[rangeIds.get(r) - 1] = r;
        }
        return ranges;
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
        Query result = null;

        for (Query q : queries) {
            if (q.name.equals(name)) {
                result = q;
                break;
            }
        }

        return result;
    }

    /**
     * Efficient representation of the Workload with primitive typed variables.
     *
     * Note that any changes in the Workload instance are not cascaded in the SimplifiedWorkload instances 
     * created before the changes.
     */
    public class SimplifiedWorkload implements Cloneable {

        public int[][] usageM;
        public int[] attributeSizes;
        public int queryCount;
        public int attributeCount;
        public int[] queryWeights; // TODO use this in some algorithms
        public long numRows;
        public int rowSize;

        public int[][] selectivityColumns;
        public double[] selectivities;

        /**
         * Constructor that creates an instance using the data of the enclosing Workload instance.
         */
        protected SimplifiedWorkload() {
            usageM = getUsageMatrix();
            attributeSizes = getAttributeSizes();
            queryCount = usageM.length;
            try {
                attributeCount = usageM[0].length;
            } catch (Exception ex) {
                attributeCount = 0;
            }
            queryWeights = getQueryWeights();
            numRows = getNumberOfRows();

            rowSize = 0;
            for (int a = 0; a < attributeCount; a++) {
                rowSize += attributeSizes[a];
            }

            if (queries.get(0).getSelectivityColumns() != null) {
                selectivityColumns = new int[queryCount][];
                selectivities = new double[queryCount];

                for (int q=0; q<queryCount; q++) {
                    selectivityColumns[q] = queries.get(q).getSelectivityColumns().clone();
                    selectivities[q] = queries.get(q).getSelectivity();
                }
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
     * @param queryName The name of the query.
     * @return The leaf-level query string.
     */
    public String getLeafLevelQueryString(String queryName) {
        StringBuilder sb = new StringBuilder();

        if (getQueryByName(queryName) == null) {
            return "";
        }

        int[] projectedAttrs = getQueryByName(queryName).getProjections();

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
