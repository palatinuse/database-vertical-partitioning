package core.algo.vertical;

import core.metrics.PartitioningCostCalculator;
import core.utils.ArrayUtils;
import core.utils.EnumUtils.Enumerate;
import core.utils.PartitioningUtils;
import db.schema.BenchmarkTables;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import db.schema.utils.WorkloadUtils;

public class Optimal extends AbstractPartitioningAlgorithm {

	public Optimal(AlgorithmConfig config) {
		super(config);
		type = Algo.OPTIMAL;
	}
	
	@Override
	public void doPartition() {
		BellNumber bn = new BellNumber();

        /***** Remove unreferenced attributes to speed-up the search. *****/
        int[] allQueries = PartitioningUtils.getPartitions(new int[w.queryCount])[0];
        int[] refAttrs = WorkloadUtils.getReferencedAttributes(w.usageMatrix, allQueries);
        Table reducedTable = BenchmarkTables.partialTable(t, refAttrs, allQueries);
        Workload.SimplifiedWorkload reducedWorkload = reducedTable.workload.getSimplifiedWorkload();
        config.setW(reducedWorkload);
        costCalculator = PartitioningCostCalculator.create(config);

		int[] subsetPartitioning = bn.enumerate(ArrayUtils.simpleArray(reducedWorkload.attributeCount, 0, 1));
        partitioning = new int[w.attributeCount];

        for (int i = 0; i < refAttrs.length; i++) {
            /* In case there are unreferenced attributes, they go to partition 0,
            and we simply shift the other partition id's by incrementing them. */
            if (refAttrs.length < w.attributeCount)
                partitioning[refAttrs[i]] = subsetPartitioning[i] + 1;
            else
                partitioning[refAttrs[i]] = subsetPartitioning[i];
        }

        /* Hack: fix empty partitions bug with consecutive partition IDs */
        partitioning = PartitioningUtils.consecutivePartitionIds(partitioning);

		profiler.numberOfIterations = bn.getNumberOfIterations();
	}
	
	
	
	/**
	 * Enumeration method
	 * 
	 * -- try all possible combinations = Bell Number
	 * 
	 */
	
	public class BellNumber extends Enumerate{
	
		protected void doIterate(){
			iterate(new int[ordering.length]);
		}
		
		protected void iterate(int[] a){
			for(int k=1;k<=a.length;k++){
				stirlingnumber(a.length, k, a);
			}
		}
		
		private void stirlingnumber(int n, int k, int[] a){
			if(k==1){
				for(int i=0;i<n;i++)
					a[i] = k-1;
				iterations++;
				//ArrayUtils.printArray(a, "", "");
				compareCost(costCalculator, PartitioningUtils.consecutivePartitionIds(a));
				return;
			}
			
			if(k==n){
				for(int i=0;i<k;i++)
					a[i] = i;
				iterations++;
				//ArrayUtils.printArray(a, "", "");
				compareCost(costCalculator, PartitioningUtils.consecutivePartitionIds(a));
				return;
			}
			
			a[n-1] = k-1;
			stirlingnumber(n-1, k-1, a);
			for(int i=0;i<k;i++){
				a[n-1] = i;
				stirlingnumber(n-1, k, a);			
			}
		}
	}
	
	public static void main(String[] args) {
		Table table = BenchmarkTables.randomTable(15, 8);
        AlgorithmConfig config = new HDDAlgorithmConfig(table);
        Optimal opt = new Optimal(config);
		Optimal.BellNumber bn = opt.new BellNumber();
		int a[] = ArrayUtils.simpleArray(5, 0, 1);
		bn.enumerate(a);
		System.out.println("Iterations="+bn.getNumberOfIterations());
	}
}
