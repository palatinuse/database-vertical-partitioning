package core.algo.vertical;

import core.utils.ArrayUtils;
import core.utils.EnumUtils.Enumerate;
import core.utils.PartitioningUtils;
import db.schema.BenchmarkTables;
import db.schema.entity.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Vertical Partitioning Algorithms for Database Design
 * Shamkant Navathe, Stefano Ceri, Gio Wiederhold, Jinglie Dou
 * 
 * ACM Transactions on Database Systems, December 1984.
 * 
 * 
 * @author alekh
 *
 */

public class NavatheAlgorithm extends AbstractPartitioningAlgorithm {

	public NavatheAlgorithm(AlgorithmConfig config){
		super(config);
		type = Algo.NAVATHE;
	}
	
	@Override
	public void doPartition() {
        if (w.queryCount > 0) {
            int[] ordering = clusterMatrix(getAffinityMatrix());
            BruteForce bf = new BruteForce();
            //--- Hack: fix empty partitions bug ---//
            partitioning = PartitioningUtils.consecutivePartitionIds(bf.enumerate(ordering));
            profiler.numberOfIterations = bf.getNumberOfIterations();
        } else {
            partitioning = new int[w.attributeCount];
        }
	}
	
	
	/**
	 * get affinity matrix
	 */
	protected int[][] getAffinityMatrix(){
		int[][] affinityMatrix = new int[w.usageMatrix[0].length][w.usageMatrix[0].length];
		for(int i=0;i<w.usageMatrix[0].length;i++){
			for(int j=0;j<=i;j++){
				int affinity = getCommonUsage(i, j);
				affinityMatrix[i][j] = affinity;
				affinityMatrix[j][i] = affinity;
			}
		}
		return affinityMatrix;
	}
	
	private int getCommonUsage(int a1, int a2){
		int counter = 0;
		for(int i=0;i<w.usageMatrix.length;i++){
			if(w.usageMatrix[i][a1]==1 && w.usageMatrix[i][a2]==1)
				counter++;
		}
		return counter;
	}
	
	/**
	 * cluster affinity matrix
	 */
	protected int[] clusterMatrix(int[][] affinityM){
		int[][] clusteredM;
		
		int size = affinityM.length;
		clusteredM = new int[size][size];
		
		// map between clustered position and affinity position (pos -> attribute Map)
		Map<Integer,Integer> posMap = new HashMap<Integer,Integer>();
		for(int i = 0;i<size;i++){
			if(i==0){
				// initialize
				clusteredM[0] = affinityM[0];
				posMap.put(0, 0);
				continue;
			}
			int maxContribution = 0;
			int maxContributionIndex = -1;
			for(int j=0; j<=i;j++){
				// find the affinity contribution of inserting Ai at position j
				Integer i_attribute = j==0 ? -1:posMap.get(j-1);
				Integer j_attribute = j==i ? -1:posMap.get(j);
				int cont = contribution(affinityM, i_attribute, j_attribute, i);
				if(cont >= maxContribution){
					maxContribution = cont;
					maxContributionIndex = j;
				}
			}
			insert(posMap, affinityM, clusteredM, i, maxContributionIndex);
		}
		
		// attribute -> pos Map
		Map<Integer,Integer> attrMap = new HashMap<Integer,Integer>();
		for(Entry<Integer,Integer> entry:posMap.entrySet())
			attrMap.put(entry.getValue(), entry.getValue());
		columnOrdering(posMap, attrMap, clusteredM);
		
		int[] ordering = new int[posMap.size()];
		for(int i=0; i<posMap.size()-1; i++)
			ordering[i] = posMap.get(i);
		
		return ordering;
	}
	
	private int contribution(int[][] affinityMatrix, int i, int j, int k){
		return (2*bond(affinityMatrix,i,k) +
				2*bond(affinityMatrix,k,j) -
				2*bond(affinityMatrix,i,j));
	}
	
	private int bond(int[][] affinityMatrix, int i, int j){
		if(i < 0 || j < 0)
			return 0;
		int sum = 0;
		for(int z=0;z<affinityMatrix.length;z++){
			sum += affinityMatrix[z][i]*affinityMatrix[z][j];
		}
		return sum;
	}
	

	private void insert(Map<Integer,Integer> posMap, int[][] affinityMatrix, int[][] clusteredMatrix, int i, int index){
		for(int x = i-1; x>=index;x--){
			clusteredMatrix[x+1] = clusteredMatrix[x];
			posMap.put(x+1, posMap.get(x));
		}
		clusteredMatrix[index] = affinityMatrix[i];;
		posMap.put(index, i);
	}
	
	private void columnOrdering(Map<Integer,Integer> posMap, Map<Integer,Integer> attrMap, int[][] clusteredMatrix){
		int[][] copyMatrix = new int[clusteredMatrix.length][clusteredMatrix.length];
		for(int i=0;i<clusteredMatrix.length;i++)
			for(int j=0;j<clusteredMatrix.length;j++)
				copyMatrix[i][j] = clusteredMatrix[i][j];
		
		for(Integer pos:posMap.keySet()){
			int currentAttrPos = attrMap.get(posMap.get(pos));
			for(int x=0;x<clusteredMatrix.length;x++){
				clusteredMatrix[x][pos] = copyMatrix[x][currentAttrPos];
			}
			attrMap.put(posMap.get(pos), pos);
		}
	}
	
	
	
	
	
	/**
	 * Enumeration methods
	 * 
	 * -- simplest is the brute force method i.e. try out all possible split vectors
	 * 
	 */
	
	public class BruteForce extends Enumerate{

		protected void doIterate(){
			iterate(0,ordering.length-1,new int[ordering.length]);
		}
		
		// brute force iteration
		protected void iterate(int x, int y, int[] a){
			for(int i=x; i<=y; i++){
				for(int k=x;k<i;k++)
					a[k] = 0;
				
				if(i < y)
					a[i] = 1;
				
				if(i < y)
					iterate(i+1, y, a);
				else{
					iterations++;
					//ArrayUtils.printArray(PartitioningUtils.getPartitioning(a, ordering), "", "");
					compareCost(costCalculator, PartitioningUtils.getPartitioning(a, ordering));
				}
			}
		}
	}
	
	public static void main(String[] args) {
		Table table = BenchmarkTables.randomTable(15, 8);
        AlgorithmConfig config = new HDDAlgorithmConfig(table);
		NavatheAlgorithm opt = new NavatheAlgorithm(config);
		NavatheAlgorithm.BruteForce bn = opt.new BruteForce();
		int a[] = ArrayUtils.simpleArray(5, 0, 1);
		bn.enumerate(a);
		System.out.println("Iterations="+bn.getNumberOfIterations());
	}
}
