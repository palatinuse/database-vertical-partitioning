package core.algo.vertical;

import core.utils.ArrayUtils;
import core.utils.EnumUtils.Enumerate;
import core.utils.ObjectUtils.Pair;
import core.utils.PartitioningUtils;
import db.schema.BenchmarkTables;
import db.schema.entity.Table;
import db.schema.utils.WorkloadUtils;
import gnu.trove.set.hash.TIntHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * 
 * Relax and Let the Database do the Partitioning Online
 * Alekh Jindal, Jens Dittrich
 * 
 * VLDB BIRTE, 2011
 * 
 * 
 * In this class we implement the O2P algorithm.
 * 
 * Note that we do not implement the Amortization and Multi-threading 
 * approaches of O2P algorithm because they can be applied for any
 * other algorithm as well, and are hence orthogonal to algorithm evaluation! 
 * 
 * @author alekh
 *
 */

public class O2P extends NavatheAlgorithm {
	
	public static enum MODE {PRUNING, GREEDY, DYNAMIC}
	
	private MODE mode;
	
	public O2P(AlgorithmConfig config) {
		super(config);
        type = Algo.O2P;
	}

	@Override
	public void doPartition() {

		int[] ordering = clusterMatrix(getAffinityMatrix());
		Enumerate e = null;
		if(mode==null)
			e = new Dynamic();
		else{
			switch(mode){
				case PRUNING:	e = new AttributePruning(); break;
				case GREEDY:	e = new Greedy(); break;
				case DYNAMIC:	e = new Dynamic(); break;
				default:		e = new Dynamic(); 
			}
		}

        //--- Hack: fix empty partitions bug ---//
		partitioning = PartitioningUtils.consecutivePartitionIds(e.enumerate(ordering));
		profiler.numberOfIterations = e.getNumberOfIterations();
	}
	
	public void setMode(MODE mode) {
		this.mode = mode;
	}

	
	
	
	
	/**
	 * Attribute Pruning technique for O2P algorithm
	 * 
	 * -- the idea is to detect the unused attributes and cut 
	 *    them out in a separate partition right away
	 * 
	 * @author alekh
	 *
	 */
	public class AttributePruning extends BruteForce{
		
		protected void doIterate(){
			TIntHashSet nonRefAtts = WorkloadUtils.getNonReferencedAttributes(w.usageMatrix);
			int[] a = new int[ordering.length];
			int x,y;
			for(x=0;x<ordering.length;x++){
				if(!nonRefAtts.contains(ordering[x]))
					break;
			}
			if(x > 0)
				a[x-1] = 1;
			for(y=ordering.length-1;y>=0;y--){
				if(!nonRefAtts.contains(ordering[y]))
					break;
			}
			if(y < (ordering.length-1))
				a[y] = 1;
			
			iterate(x,y,new int[ordering.length]);
		}
	}
	
	
	
	/**
	 * Greedy enumeration in O2P algorithm
	 * 
	 * -- the idea is to greedily pick one split line
	 *    at a time; repeat this process recursively
	 *    
	 *    
	 * @author alekh
	 *
	 */
	public class Greedy extends Enumerate{
		
		protected void doIterate(){
			iterate(new HashSet<Integer>(),new int[ordering.length]);
		}
		
		private void iterate(Set<Integer> taken, int[] a){
			boolean maxPartitioned = true;
			double localMinCost = Double.MAX_VALUE;
			int i_prime = -1;
			
			for(int i=0;i<(a.length-1);i++){
				if(!taken.contains(i)){
					a[i] = 1;
					double c = compareCost(costCalculator, PartitioningUtils.getPartitioning(a, ordering));
					if(c < localMinCost){
						i_prime = i;
						localMinCost = c;
					}
					a[i] = 0;
					maxPartitioned = false;
					iterations++;
				}
			}
			if(maxPartitioned)
				return;
			else{
				a[i_prime] = 1;
				taken.add(i_prime);
				iterate(taken,a);
			}
		}
	}
	
	
	
	public class Dynamic extends Enumerate{
		
		protected void doIterate(){
			iterate(new HashMap<Integer,Pair<Integer,Double>>(), 0, ordering.length-1, new int[ordering.length]);
		}
		
		private boolean iterate(Map<Integer,Pair<Integer,Double>> perSplitOptimal, int prevSplitLeft, int prevSplitRight, int[] a){
			
			// find the best partition in the "left" partition of the previous split
			double c_min_left = Double.MAX_VALUE;
			int i_prime_left = -1;
			for(int i=prevSplitLeft;i<(a.length-1);i++){
				if(a[i]==1)
					break;
				
				// compute the cheapest split within this partition
				a[i] = 1;
				//System.out.print(" -- ");
				//printArray(a);
				double newLocalCost = compareCost(costCalculator, PartitioningUtils.getPartitioning(a, ordering));
				if(newLocalCost < c_min_left){
					i_prime_left = i;
					c_min_left = newLocalCost;
				}
				a[i] = 0;
				iterations++;
			}
			
			// find the best partition in the "right" partition of the previous split
			double c_min_right = Double.MAX_VALUE;
			int i_prime_right = -1;
			for(int i=prevSplitRight;i<(a.length-1);i++){
				if(a[i]==1)
					break;
				
				// compute the cheapest split within this partition
				a[i] = 1;
				//System.out.print(" -- ");
				//printArray(a);
				double newLocalCost = compareCost(costCalculator, PartitioningUtils.getPartitioning(a, ordering));
				if(newLocalCost < c_min_right){
					i_prime_right = i;
					c_min_right = newLocalCost;
				}
				a[i] = 0;
				iterations++;
			}
			
			// now compute the minimum cost amongst other partitions
			double c_min_prev = Double.MAX_VALUE;
			int i_prime_prev = -1;
			int i_split_optimal = -1;
			for(Integer i: perSplitOptimal.keySet()){
				if(perSplitOptimal.get(i).second < c_min_prev){
					c_min_prev = perSplitOptimal.get(i).second;
					i_prime_prev = perSplitOptimal.get(i).first;
					i_split_optimal = i;
				}
			}
			
			
			// now compare the minimum costs from the three places
			double c_min = Double.MAX_VALUE;
			if(i_prime_left >=0)
				c_min = Math.min(c_min, c_min_left);
			if(i_prime_right >= 0)
				c_min = Math.min(c_min, c_min_right);
			if(i_prime_prev >= 0)
				c_min = Math.min(c_min, c_min_prev);
			
			if(c_min == Double.MAX_VALUE){
				// max partitioned
				return true;
			}
			else if(c_min == c_min_left){
				a[i_prime_left] = 1;
				//printArray(a);
				if(i_prime_right >= 0)
					perSplitOptimal.put(prevSplitRight, new Pair<Integer,Double>(i_prime_right,c_min_right));
				iterate(perSplitOptimal, prevSplitLeft, i_prime_left+1, a);
				//dynamicAmmortizedPerSplitOptimal = perSplitOptimal;
				//dynamicAmmortizedPrevSplitLeft = prevSplitLeft;
				//dynamicAmmortizedPrevSplitRight = i_prime_left+1;
				//dynamicAmmortizedPartitioning = a;
			}
			else if(c_min == c_min_right){
				a[i_prime_right] = 1;
				//printArray(a);
				if(i_prime_left >= 0)
					perSplitOptimal.put(prevSplitLeft, new Pair<Integer,Double>(i_prime_left,c_min_left));
				iterate(perSplitOptimal, prevSplitRight, i_prime_right+1, a);
				//dynamicAmmortizedPerSplitOptimal = perSplitOptimal;
				//dynamicAmmortizedPrevSplitLeft = prevSplitRight;
				//dynamicAmmortizedPrevSplitRight = i_prime_right+1;
				//dynamicAmmortizedPartitioning = a;
			}
			else if(c_min == c_min_prev){
				a[i_prime_prev] = 1;
				//printArray(a);
				if(i_prime_right >= 0)
					perSplitOptimal.put(prevSplitRight, new Pair<Integer,Double>(i_prime_right,c_min_right));
				if(i_prime_left >= 0)
					perSplitOptimal.put(prevSplitLeft, new Pair<Integer,Double>(i_prime_left,c_min_left));
				perSplitOptimal.remove(i_split_optimal);
				iterate(perSplitOptimal, i_split_optimal, i_prime_prev+1, a);
				//dynamicAmmortizedPerSplitOptimal = perSplitOptimal;
				//dynamicAmmortizedPrevSplitLeft = i_split_optimal;
				//dynamicAmmortizedPrevSplitRight = i_prime_prev+1;
				//dynamicAmmortizedPartitioning = a;
			}
			else{
				// bug
			}
			return false;
		}
		
	}
	
	public static void main(String[] args) {
		Table table = BenchmarkTables.randomTable(15, 8);
        AlgorithmConfig config = new HDDAlgorithmConfig(table);
        O2P opt = new O2P(config);
		O2P.Greedy bn = opt.new Greedy();
		int a[] = ArrayUtils.simpleArray(5, 0, 1);
		bn.enumerate(a);
		System.out.println("Iterations="+bn.getNumberOfIterations());
	}
}
