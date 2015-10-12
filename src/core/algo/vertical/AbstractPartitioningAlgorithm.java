package core.algo.vertical;

import core.metrics.PartitioningCostCalculator;
import core.metrics.PartitioningProfiler;
import core.utils.PartitioningUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public abstract class AbstractPartitioningAlgorithm extends AbstractAlgorithm {

	protected PartitioningCostCalculator costCalculator;
	protected PartitioningProfiler profiler;

	protected int[] partitioning;
	
	public AbstractPartitioningAlgorithm(AlgorithmConfig config) {
		super(config);
		costCalculator = PartitioningCostCalculator.create(config);
		profiler = new PartitioningProfiler(w);
	}

    /**
     * Factory method to create a row layout partitioning.
     * @param numAttributes The number of attributes in the table
     * @return The partitioning in row layout
     */
    public static int[] rowLayout(int numAttributes) {
        return new int[numAttributes];
    }

    /**
     * Factory method to create a column layout partitioning.
     * @param numAttributes The number of attributes in the table
     * @return The partitioning in column layout
     */
    public static int[] columnLayout(int numAttributes) {
        int[] partitioning = new int[numAttributes];

        for (int i=0; i<numAttributes; i++) {
            partitioning[i] = i;
        }

        return partitioning;
    }
    
    /*
    @Override
    public double getEstimatedCost() {
        return costCalculator.getPartitioningCost(partitioning);
    }

    @Override
    public double getEstimatedSeekCost() {
        return costCalculator.getPerQueryPartitionsCosts(PartitioningUtils.getPartitions(partitioning))[CostModel.SEEK];
    }

    @Override
    public double getEstimatedScanCost() {
        return costCalculator.getPerQueryPartitionsCosts(PartitioningUtils.getPartitions(partitioning))[CostModel.SCAN];
    }
	
	@Override
	public int getRedundantBytesRead() {		
		return profiler.redundantBytesReadPerRow(partitioning);
	}

	@Override
	public int getAttributeJoins() {		
		return profiler.attributeJoinsPerRow(partitioning);
	} */

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
		return PartitioningUtils.getPartitioningMap(partitioning);
	}

	public int[] getPartitioning() {
		return partitioning;
	}
}
