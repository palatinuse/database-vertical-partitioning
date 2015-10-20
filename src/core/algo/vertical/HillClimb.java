package core.algo.vertical;

import core.utils.PartitioningUtils;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Data Morphing: An Adaptive, Cache-Conscious Storage Technique
 * Richard A. Hankins, Jignesh M. Patel
 * 
 * VLDB September, 2003.
 * 
 * 
 * 
 * @author vladimir
 *
 */
public class HillClimb extends AbstractPartitioningAlgorithm {
	/*
	 * We do not use a cost table (as in the original algorithm)
	 * because the table becomes too big for large number of 
	 * attributes (~16GB for 46 attributes). Instead, it is not very
	 * expensive to calculate the costs repeatedly.
	 */
	//	private Map<String, Double> costTable;
	
	public HillClimb(AlgorithmConfig config) {
		super(config);
		type = Algo.HILLCLIMB;
		
//		costTable = new HashMap<String, Double>();
	}
	
	@Override
	public void doPartition() {
//		int[][] allGroups = getSetOfGroups(usageMatrix);
//		
//		for (int[] group : allGroups) {			
//			costTable.put(Arrays.toString(group), cm.getPartitionsCost(group));
//		}
		
		int[][] cand = new int[w.attributeCount][1];
		for(int i = 0; i < w.attributeCount; i++) {
			cand[i][0] = i;
		}
		double candCost = getCandCost(cand);
		double minCost;
		List<int[][]> candList = new ArrayList<int[][]>();
		int[][] R;
		int[] s;
		do {
			R = cand;
			minCost = candCost;
			candList.clear();
			for (int i = 0; i < R.length; i++) {
				for (int j = i + 1; j < R.length; j++) {
					cand = new int[R.length-1][];
					s = doMerge(R[i], R[j]);
					for(int k = 0; k < R.length; k++) {
						if(k == i) {
							cand[k] = s;
						} else if(k < j) {
							cand[k] = R[k];
						} else if(k > j) {
							cand[k-1] = R[k];							
						}
					}
					candList.add(cand);
				}
			}
			if(!candList.isEmpty()) {
				cand = getLowerCostCand(candList);
				candCost = getCandCost(cand);
			}
		} while (candCost < minCost);

		partitioning = PartitioningUtils.getPartitioning(R);
	}
	
	private int[][] getLowerCostCand(List<int[][]> candList) {
		int indexOfLowest = 0;
		int index = 0;
		double lowestCost = Double.MAX_VALUE;
		for (int[][] cand : candList) {
			double cost = getCandCost(cand);
			if (lowestCost > cost) {
				indexOfLowest = index;
				lowestCost = cost;
			}
			index++;
		}
		return candList.get(indexOfLowest);
	}

	private int[] doMerge(int[] is, int[] is2) {

		TIntHashSet set = new TIntHashSet();
		set.addAll(is);
	    set.addAll(is2);

		return set.toArray();
	}

	private double getCandCost(int[][] cand) {
		double sum = 0;

        sum = costCalculator.getPartitionsCost(cand);

        /*
		for (int[] item : cand) {
			sum += costCalculator.costForPartition(item);
//			System.out.println(Arrays.toString(item));
//			sum += costTable.get(Arrays.toString(item));
		} */

		return sum;
	}

//	private int[][] getSetOfGroups(int[][] usageMatrix) {
//		Map<Integer, List<Integer>> partitionAttributes = new HashMap<Integer,List<Integer>>();
//		List<Integer> attributes = new ArrayList<Integer>();
//		for(int i = 0; i < usageMatrix[0].length; i++)
//			attributes.add(i);
////		System.out.println("attrSize: "+attributes.size());
//		List<List<Integer>> psetattr = powerSetIter(attributes);
//		Collections.sort(psetattr, new ListComparator());
//		
//		int partitionCount = 0;
//		for (int p = psetattr.size()-1; p >= 0 ; p--) {
//			partitionAttributes.put(partitionCount++, psetattr.get(p));			
//		}
//				
//		int[][] primaryPartitions = new int[partitionAttributes.size()][];
//		int i = 0;
//		for(int p : partitionAttributes.keySet()){
//			List<Integer> attrs = partitionAttributes.get(p);
//			primaryPartitions[i] = new int[attrs.size()];
//			for(int j = 0; j < attrs.size(); j++)
//				primaryPartitions[i][j] = attrs.get(j);
//			i++;
//		}
//		
//		return primaryPartitions;
//	}
//
//	
//	public class ListComparator implements Comparator<List<Integer>> {
//	    @Override
//	    public int compare(List<Integer> o1, List<Integer> o2) {
//	        return o2.size()-o1.size();
//	    }
//	}
//
//	public static <T> List<List<T>> powerSetIter(Collection<T> list) {
//		List<List<T>> ps = new ArrayList<List<T>>();
//		ps.add(new ArrayList<T>()); // add the empty set
//
//		// for every item in the original list
//		for (T item : list) {
//			List<List<T>> newPs = new ArrayList<List<T>>();
//
//			for (List<T> subset : ps) {
//				// copy all of the current powerset's subsets
//				newPs.add(subset);
//
//				// plus the subsets appended with the current item
//				List<T> newSubset = new ArrayList<T>(subset);
//				newSubset.add(item);
//				newPs.add(newSubset);
//			}
//
//			// powerset is now powerset of list.subList(0, list.indexOf(item)+1)
//			ps = newPs;
//		}
//		ps.remove(new ArrayList<T>()); // remove the empty set
//		return ps;
//	}
}
