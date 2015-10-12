package core.algo.vertical;

import core.metrics.PartitionsCostCalculator;
import core.metrics.PartitionsProfiler;
import core.utils.PartitioningUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public abstract class AbstractPartitionsAlgorithm extends AbstractAlgorithm {

	protected PartitionsCostCalculator costCalculator;
	protected PartitionsProfiler profiler;

	protected TIntObjectHashMap<TIntHashSet> partitions;
    protected TIntObjectHashMap<TIntHashSet> bestSolutions;

	public AbstractPartitionsAlgorithm(AlgorithmConfig config) {
		super(config);

        costCalculator = PartitionsCostCalculator.create(config);
		profiler = new PartitionsProfiler(w);
	}

    /**
     * Factory method to create a row layout partitioning.
     * @param numAttributes The number of attributes in the table
     * @return The partitioning in row layout
     */
    public static TIntObjectHashMap<TIntHashSet> rowLayout(int numAttributes) {
        return PartitioningUtils.getPartitioningMap(new int[numAttributes]);
    }

    /**
     * Factory method to create a column layout partitioning.
     * @param numAttributes The number of attributes in the table
     * @return The partitioning in column layout
     */
    public static TIntObjectHashMap<TIntHashSet> columnLayout(int numAttributes) {
        return PartitioningUtils.getPartitioningMap(AbstractPartitioningAlgorithm.columnLayout(numAttributes));
    }

	@Override
	public long getCandidateSetSize() {
		return profiler.candidateSetSize;
	}

	@Override
	public long getNumberOfIterations() {
		return profiler.numberOfIterations;
	}

	@Override
	public TIntObjectHashMap<TIntHashSet> getPartitions() {
		return partitions;
	}

    public TIntObjectHashMap<TIntHashSet> getBestSolutions() {
        return bestSolutions;
    }

}
