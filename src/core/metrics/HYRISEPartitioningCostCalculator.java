package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.MMCostModel;
import core.utils.PartitioningUtils;

/**
 * Cost calculator according to the HYRISE paper.
 *
 * @author Alekh Jindal
 */
public class HYRISEPartitioningCostCalculator extends PartitioningCostCalculator {

    private MMCostModel cm;

    public HYRISEPartitioningCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
        super(config);

        cm = new MMCostModel(w);
    }

    @Override
    public double getPartitionsCost(int[][] partitions, int[] queries) {

        long totalCacheMisses = 0;
        for(int[] partition: partitions){
            for(int query: queries)
                totalCacheMisses += cm.getCostForPartition(partition, query);
        }

        return totalCacheMisses;
    }

    public double getPartitioningCost(int[] partitioning, int[] queries) {
    	int[][] partitions = PartitioningUtils.getPartitions(partitioning);

    	return getPartitionsCost(partitions, queries);
    }
}
