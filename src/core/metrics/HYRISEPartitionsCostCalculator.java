package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.MMCostModel;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Cost calculator for overlapping partitionings and the HYRISE cost model.
 */
public class HYRISEPartitionsCostCalculator extends PartitionsCostCalculator {

    private MMCostModel cm;

    public HYRISEPartitionsCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
        super(config);

        cm = new MMCostModel(w);
    }

    @Override
    public double getPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {

        long totalCacheMisses = 0;

        for (int q = 0; q < w.queryCount; q++) {
            for (int p : bestSolutions.get(q).toArray()) {
                TIntArrayList partition = new TIntArrayList(partitions.get(p).toArray());
                partition.sort();

                totalCacheMisses += cm.getCostForPartition(partition.toArray(), q);
            }
        }

        // TODO factor in the cost of a single cache miss!
        return  1.0 * totalCacheMisses;
    }

    @Override
    public double findPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions) {
        workload.setBestSolutions(new TIntObjectHashMap<TIntHashSet>());

        double cost = 0.0;

        for (int q = 0; q < w.queryCount; q++) {

            // the attributes referenced by the query
            TIntArrayList queryAccessSet = new TIntArrayList(w.attributeCount);

            for (int a = 0; a < w.attributeCount; a++) {
                if (w.usageMatrix[q][a] == 1) {
                    queryAccessSet.add(a);
                }
            }

            // which partitions can be used for a given referenced attribute
            TIntObjectHashMap<TIntHashSet> candidatePartitions = new TIntObjectHashMap<TIntHashSet>();

            for (TIntIterator ait = queryAccessSet.iterator(); ait.hasNext();) {
                int a = ait.next();
                candidatePartitions.put(a, new TIntHashSet(w.attributeCount));

                for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext();) {
                    int p = pit.next();

                    if (partitions.get(p).contains(a)) {
                        candidatePartitions.get(a).add(p);
                    }
                }
            }

            HYRISEPartitionSelectionPlanSolver ocm = new HYRISEPartitionSelectionPlanSolver(
                    partitions, queryAccessSet, candidatePartitions, q);
            ocm.findBestPartitions(0, new TIntHashSet(w.attributeCount), new TIntHashSet(w.attributeCount));

            cost += ocm.getBestCost();
            workload.getBestSolutions().put(q, ocm.getBestSolution());
        }

        return cost;
    }

    /**
     * Class for finding the best subset of overlapping partitions to be used
     * for a given query.
     */
    private class HYRISEPartitionSelectionPlanSolver {

        private TIntObjectHashMap<TIntHashSet> partitions;
        private TIntArrayList queryAccessSet;
        private TIntObjectHashMap<TIntHashSet> candidatePartitions;
        private int query;

        private double bestCost;
        private TIntHashSet bestSolution;

        /**
         * Constructor
         * @param partitions The current partitions of the table.
         * @param queryAccessSet The set of attributes referenced by the query.
         * @param candidatePartitions The partitions that contain attributes referenced by the actual query.
         * @param query The actual query.
         */
        public HYRISEPartitionSelectionPlanSolver(TIntObjectHashMap<TIntHashSet> partitions, TIntArrayList queryAccessSet,
                                               TIntObjectHashMap<TIntHashSet> candidatePartitions, int query) {

            this.partitions = partitions;
            this.queryAccessSet = queryAccessSet;
            this.candidatePartitions = candidatePartitions;
            this.query = query;
        }

        private double getPartitionsCostForQuery(TIntHashSet currentSolution) {
            long totalCacheMisses = 0;

            for (int p : currentSolution.toArray()) {
                TIntArrayList partition = new TIntArrayList(partitions.get(p).toArray());
                partition.sort();

                totalCacheMisses += cm.getCostForPartition(partition.toArray(), query);
            }

            // TODO factor in the cost of a single cache miss!
            return  1.0 * totalCacheMisses;
        }

        /**
         * Method for finding the best subset of overlapping partitions to be
         * used for a given query. It enumerates all possible solutions
         * recursively.
         *
         * It stores the best cost and the best solution in local variables.
         *
         * @param index
         *            The index of the actual attribute in the queryAccessSet
         *            for which we try to find a solution.
         * @param alreadyCovered
         *            The attributes that are already covered by the current
         *            partial solution.
         * @param currentSolution
         *            The partition IDs chosen.
         */
        public void findBestPartitions(int index, TIntHashSet alreadyCovered, TIntHashSet currentSolution) {

            if (index == 0) {
                bestCost = Double.MAX_VALUE;
            }

            for (TIntIterator pit = candidatePartitions.get(queryAccessSet.get(index)).iterator(); pit.hasNext();) {
                int p = pit.next();

                TIntHashSet partition = partitions.get(p);
                TIntHashSet intersection = new TIntHashSet(partition);
                intersection.retainAll(queryAccessSet);

                TIntHashSet newCurrentSolution = new TIntHashSet(currentSolution);
                TIntHashSet newAlreadyCovered = new TIntHashSet(alreadyCovered);

                if (!alreadyCovered.containsAll(intersection)) {
                    newAlreadyCovered.addAll(intersection);
                    newCurrentSolution.add(p);
                }

                /*
                 * the attribute with the last index has just been covered, or
                 * even before that all of them have been covered
                 */
                if (index == queryAccessSet.size() - 1 || newAlreadyCovered.size() == queryAccessSet.size()) {

                    // cost calculation
                    double cost = getPartitionsCostForQuery(newCurrentSolution);

                    if (cost < bestCost) {
                        bestCost = cost;
                        bestSolution = newCurrentSolution;
                    }

                } else {
                    findBestPartitions(index + 1, newAlreadyCovered, newCurrentSolution);
                }
            }

            if (alreadyCovered.contains(queryAccessSet.get(index))) {
                getPartitionsCostForQuery(currentSolution);
            }
        }

        public double getBestCost() {
            return bestCost;
        }

        public TIntHashSet getBestSolution() {
            return bestSolution;
        }
    }
}
