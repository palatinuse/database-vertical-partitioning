package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import db.schema.entity.Workload;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for cost computations for algorithms producing possibly overlapping partitions.
 * 
 * @author Endre Palatinus
 */
public abstract class PartitionsCostCalculator {
	
	protected Workload.SimplifiedWorkload w;
	protected Workload workload;
	
	public PartitionsCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
		this.workload = config.getTable().workload;
		this.w = config.w;
	}

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return The calculated cost.
     */
    public abstract double getPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions,
                                             TIntObjectHashMap<TIntHashSet> bestSolutions);

    /**
     * Calculate the sum of the I/O seek cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return The calculated cost.
     */
    public double getPartitionsSeekCost(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {
        return getPartitionsCosts(partitions, bestSolutions)[CostModel.SEEK];
    }

    /**
     * Calculate the sum of the I/O scan cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return The calculated cost.
     */
    public double getPartitionsScanCost(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {
        return getPartitionsCosts(partitions, bestSolutions)[CostModel.SCAN];
    }

    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return An array with 2 doubles, the first being the seek- and the second the scan cost.
     */
    public double[] getPartitionsCosts(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {

        double seekCost = 0.0;    // Quick fix for some experiments.
        double scanCost = 0.0;

        double[] costs = new double[2];
        costs[CostModel.SEEK] = seekCost;
        costs[CostModel.SCAN] = scanCost;

        return costs;
    }

	/**
	 * Method for calculating the cost of a possibly overlapping partitioning.
	 * Note that this method overwrites the value of rowSize and refRowSize
	 * according to the partitioning, and stores the best partitions to be used
	 * for executing a query.
	 * 
	 * @param partitions
	 *            The map of partitions.
	 * @return The calculated cost.
	 */
	public abstract double findPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions);

    /**
     * Method for calculating the cost of transforming the current layout to the newly calculated partitioning.
     * @param sourcePartitions The current layout.
     * @param targetPartitions The newly calculated layout.
     * @return The cost of reading the current layout and writing the target layout.
     */
    public double layoutCreationCost(TIntObjectHashMap<TIntHashSet> sourcePartitions,
                                     TIntObjectHashMap<TIntHashSet> targetPartitions) {

        double cost = 0.0;  // Quick fix for some experiments.

        return cost;
    }

    public static PartitionsCostCalculator create(AbstractAlgorithm.AlgorithmConfig config) {
        switch (config.type) {
            case HDD:
                //AbstractAlgorithm.HDDAlgorithmConfig HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
                return new HDDPartitionsCostCalculator(config);
            case HDDSelectivity:
                //AbstractAlgorithm.HDDAlgorithmConfig HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
                return new HDDSelectivityPartitionsCostCalculator(config);
            case MM:
                return new HYRISEPartitionsCostCalculator(config);
            default:
                return null;
        }
    }
}
