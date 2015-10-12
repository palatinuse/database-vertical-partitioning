package core.utils;

import core.metrics.PartitioningCostCalculator;

public class EnumUtils {

	public static class Enumerate{
		
		protected int iterations = 0;
		protected int[] ordering;
		protected double minCost = Double.MAX_VALUE;
		protected int[] bestPartitioning;
		
		public  int[] enumerate(int[] ordering){
			this.ordering = ordering;
			doIterate();
			return bestPartitioning;
		}
		
		protected void doIterate(){
			throw new UnsupportedOperationException();
		}
		
		public int getNumberOfIterations(){
			return this.iterations;
		}
		
		protected double compareCost(PartitioningCostCalculator costCalculator, int[] partitioning){
			double c = costCalculator.getPartitioningCost(partitioning);
			if(c < minCost){
				bestPartitioning = partitioning;
				minCost = c; 
			}
			return c;
		}
	}
	
}
