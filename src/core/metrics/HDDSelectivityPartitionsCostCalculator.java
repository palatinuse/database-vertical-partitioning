package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.utils.PartitioningUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for cost computations for algorithms producing possibly overlapping partitions, considering selectivity as well.
 * NOTE: This is only a quick fix for testing and falls back to HDDSelectivityPartitioningCostCalculator inside.
 *
 * @author Endre Palatinus
 */
public class HDDSelectivityPartitionsCostCalculator extends PartitionsCostCalculator {

    private HDDSelectivityPartitioningCostCalculator costCalculator;

    public HDDSelectivityPartitionsCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
        super(config);

        AbstractAlgorithm.HDDAlgorithmConfig HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
        costCalculator = new HDDSelectivityPartitioningCostCalculator(HDDconfig);
    }

    @Override
    public double getPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {
        return costCalculator.getPartitionsCost(PartitioningUtils.getPartitions(partitions));
    }

    @Override
    public double findPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions) {
        return costCalculator.getPartitionsCost(PartitioningUtils.getPartitions(partitions));
    }
}
