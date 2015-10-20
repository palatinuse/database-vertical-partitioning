package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import core.utils.PartitioningUtils;
import db.schema.entity.Workload;

/**
 * Class for cost computations for algorithms producing a non-overlapping partitioning.
 *
 * @author Endre Palatinus
 */
public abstract class PartitioningCostCalculator {

    public CostModel.CMType type;

    protected Workload.SimplifiedWorkload w;
    protected int[] allQueries;

    public PartitioningCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
        this.w = config.w;

        allQueries = new int[w.queryCount];
        for (int i = 0; i < allQueries.length; i++) {
            allQueries[i] = i;
        }
    }

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     */
    public double getPartitioningCost(int[] partitioning) {
        return getPartitionsCost(PartitioningUtils.getPartitions(partitioning), allQueries);
    }

    /**
     * Calculate the sum of the I/O seek cost through all queries imposed by all partitions.
     */
    public double getPartitioningSeekCost(int[] partitioning) {
        return 0.0; // Quick fix for some experiments.
    }

    /**
     * Calculate the sum of the I/O scan cost through all queries imposed by all partitions.
     */
    public double getPartitioningScanCost(int[] partitioning) {
        return 0.0; // Quick fix for some experiments.
    }

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     */
    public double getPartitionsCost(int[][] partitions) {
        return getPartitionsCost(partitions, allQueries);
    }

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     * @return An array with 2 doubles, the first being the seek- and the second the scan cost.
     */
    public double[] getPartitionsCosts(int[][] partitions, int[] queries) {

        double[] totalCosts = new double[] {0.0, 0.0}; // Quick fix for some experiments.
        return totalCosts;
    }

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     */
    public abstract double getPartitionsCost(int[][] partitions, int[] queries);

    /**
     * Method for calculating the cost of transforming the current layout to the newly calculated partitioning.
     * @param sourcePartitioning The current layout.
     * @param targetPartitioning The newly calculated layout.
     * @return The cost of reading the current layout and writing the target layout.
     */
    public double layoutCreationCost(int[] sourcePartitioning, int[] targetPartitioning) {
        double cost = 0.0; // Quick fix for some experiments.

        return cost;
    }

    /**
     * Method for calculating the cost of creating the layout for the newly calculated partitioning
     * by filling it with data read from a file.
     * @param targetPartitioning The newly calculated layout.
     * @return The cost of reading the file and writing date into the target layout.
     */
    public double layoutCreationCost(int[] targetPartitioning) {
        double cost = 0.0; // Quick fix for some experiments.

        return cost;
    }

    public static PartitioningCostCalculator create(AbstractAlgorithm.AlgorithmConfig config) {
        AbstractAlgorithm.HDDAlgorithmConfig HDDconfig;

        switch (config.type) {
            case HDD:
                HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
                return new HDDPartitioningCostCalculator(HDDconfig);
            case HDDSelectivity:
                HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
                return new HDDSelectivityPartitioningCostCalculator(HDDconfig);
            case MM:
                return new HYRISEPartitioningCostCalculator(config);
            default:
                return null;
        }
    }
}
