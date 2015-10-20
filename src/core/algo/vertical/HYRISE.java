package core.algo.vertical;

import core.utils.ArrayUtils;
import core.utils.PartitioningUtils;

import java.io.*;
import java.math.BigInteger;
import java.util.*;

/**
 * HYRISE - A Matek Memory Hybrid Storage Engine
 * Martin Grund, Jens Krueger, Hasso Plattner, Alexander Zeier, Philippe Cudre-Mauroux, Samuel Madden
 * 
 * PVLDB September, 2010.
 * 
 * 
 * 
 * @author alekh
 *
 */
public class HYRISE extends AbstractPartitioningAlgorithm {

	private double minCost = Double.MAX_VALUE;
	private int[][] bestLayout;
	
	private static int K = 3;	// number of partitions for k-way partitioner
	private static String graphFileName = "HYRISE_data_file";
	private static String partitionFileName = "HYRISE_data_file.part."+K;
	private static String metisLocation = "lib/metis/kmetis";
	
	public HYRISE(AlgorithmConfig config) {
		super(config);
		type = Algo.HYRISE;
	}
	
	public void doPartition() {

        /* HACK: if usageMatrix is all 1s, than no need to execute the algorithm: */
        int sum = 0;
        for (int q = 0; q < w.queryCount; q++) {
            for (int a = 0; a < w.attributeCount; a++) {
                sum += w.usageMatrix[q][a];
            }
        }

        if (sum == w.queryCount * w.attributeCount) {
            partitioning = new int[w.attributeCount];
            return;
        }

		//ArrayUtils.printArray(usageMatrix, "Usage Matrix", "Query", null);
		
		// generate candidate partitions (all possible primary partitions)
		int[][] candidatePartitions = generateCandidates(w.usageMatrix);
		//ArrayUtils.printArray(candidatePartitions, "Number of primary partitions:"+candidatePartitions.length, "Partition ", null);
		
		// generate the affinity matrix and METIS input file 
		int[][] matrix = affinityMatrix(candidatePartitions, w.usageMatrix);
		//ArrayUtils.printArray(matrix, "Partition Affinity Matrix Size: "+matrix.length, null, null);
		writeGraphFile(graphFileName, matrix);
		
		// run METIS using the graph file
		try {
			Process p = Runtime.getRuntime().exec(metisLocation+" "+graphFileName+" "+K);
			p.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		
		// read primary partition groups created by METIS 
		int[][][] primaryPartitionGroups = readPartitionFile(candidatePartitions); // TODO exception here for too few groups or attributes...
		//for(int[][] primaryPartitionGroup: primaryPartitionGroups)
		//	ArrayUtils.printArray(primaryPartitionGroup, "Partition Group", "Partition", null);
		
		
		//int i=0;
		List<int[][]> bestLayouts = new ArrayList<int[][]>();
		for(int[][] primaryPartitionGroup: primaryPartitionGroups){
			minCost = Double.MAX_VALUE;
			
			// Candidate Merging
			//System.out.println("Primary Partition Group:"+(i+1));
			List<BigInteger> mergedCandidatePartitions = mergeCandidates(primaryPartitionGroup);
			//System.out.println("Number of merged partitions:"+mergedCandidatePartitions.size());
			
			// Layout Generation
			generateLayout(mergedCandidatePartitions, primaryPartitionGroup);
			bestLayouts.add(bestLayout);			
			//ArrayUtils.printArray(bestLayout, "Layout:"+(++i), null, null);
		}
		
		// Combine sub-Layouts
		List<int[][]> mergedBestLayouts = mergeAcrossLayouts(bestLayouts, bestLayouts.size());
		partitioning = PartitioningUtils.getPartitioning(mergedBestLayouts);
	}
	
	private int coAccessCount(int[][] usageM, int a, int b){
		int count = 0;
		for(int[] q: usageM){
			int c = 0;
			for(int i=0;i<q.length;i++){
				if(q[i]==1){
					if(i==a)
						c++;
					if(i==b)
						c++;
				}
			}
			if(c >= 2)
				count++;
		}
		return count;
	}
	
	private int[][] affinityMatrix(int[][] primaryPartitions, int[][] usageM){
		int[][] matrix = new int[primaryPartitions.length][primaryPartitions.length];
		for(int i=0;i<matrix.length;i++){
			for(int j=0;j<matrix[i].length;j++){
				matrix[i][j] = coAccessCount(usageM, primaryPartitions[i][0], primaryPartitions[j][0]);
			}
		}
		return matrix;
	}

	/**
	 * Generate the input graph file for Metis (multilevel k-way partitioner)
	 * 
	 * @param filename
	 * @param matrix
	 */
	private void writeGraphFile(String filename, int[][] matrix){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(filename));
			
			int[][] orphan = new int[matrix.length][matrix[0].length];
			
			int edges = 0;
			for(int i=0;i<matrix.length;i++){
				for(int j=0;j<i;j++)
					if(matrix[i][j]!=0)
						edges++;
				
				// check if the ith element is an orphan
				int references = 0;
				for(int j=0;j<matrix[i].length;j++){
					if(i!=j)
						references += matrix[i][j];
				}
				if(references==0 && orphan[i][(i+1)%matrix.length]==0){
					// orphan
					orphan[i][(i+1)%matrix.length] = 1;
					orphan[(i+1)%matrix.length][i] = 1;
					edges++;
				}
			}
			
			writer.write(matrix.length+" "+edges+" 1");
			writer.newLine();
			
			for(int i=0;i<matrix.length;i++){
				String rowString = "";
				for(int j=0;j<matrix[i].length;j++){
					if(i!=j && (matrix[i][j]!=0 || orphan[i][j]!=0)){
						rowString += (j+1)+" "+matrix[i][j];
						if(j < matrix[i].length-1)
							rowString += " ";
					}
				}
				writer.write(rowString);
				if(i < matrix.length-1)
					writer.newLine();
			}
			writer.flush();
			writer.close();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private int[][][] readPartitionFile(int[][] candidatePartitions){
		// read partition file
		try {
			BufferedReader reader = new BufferedReader(new FileReader(partitionFileName));
			Map<Integer,List<Integer>> partitionGroups = new HashMap<Integer,List<Integer>>();
			int partitionIdx = 0;
			while(true){
				String line = reader.readLine();
				if(line==null || line.trim().equals(""))
					break;
				int groupId = Integer.parseInt(line.trim());
				if(!partitionGroups.containsKey(groupId)){
					partitionGroups.put(groupId, new ArrayList<Integer>());
				}
				partitionGroups.get(groupId).add(partitionIdx++);
			}
			
			int[][][] primaryPartitionGroups = new int[partitionGroups.size()][][];
			int serialGroupId = 0; 
			for(Integer groupId: partitionGroups.keySet()){
				List<Integer> partitionIdxes = partitionGroups.get(groupId);
				primaryPartitionGroups[serialGroupId] = new int[partitionIdxes.size()][];
				for(int i=0;i<partitionIdxes.size();i++){
					primaryPartitionGroups[serialGroupId][i] = candidatePartitions[partitionIdxes.get(i)];
				}
				serialGroupId++;
			}
			return primaryPartitionGroups;
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
		
	
	/*
	 * Generate primary partitions.
	 *  A primary partition is a partition that does not incur any "container overhead" cost.
	 * 
	 */
	private int[][] generateCandidates(int[][] usageM){
		// 1. start with the complete set of attributes
		Map<Integer, List<Integer>> partitionAttributes = new HashMap<Integer,List<Integer>>();
		List<Integer> attributes = new ArrayList<Integer>();
		for(int i=0;i<usageM[0].length;i++)
			attributes.add(i);
		partitionAttributes.put(0, attributes);
		
		// 2. for each query, split this set of attributes into two set:
		//    (i) attributes accessed by the query
		//	  (ii) attributes not-accessed by the query
		for(int[] query: usageM){
			Map<Integer, List<Integer>> newPartitionAttributes = new HashMap<Integer,List<Integer>>();
			int partitionCount = partitionAttributes.size();
			
			// check each partition
			for(int p: partitionAttributes.keySet()){
				List<Integer> attrs = partitionAttributes.get(p);
				boolean fullPartitionAccess = true;
				for(int a: attrs){
					if(query[a]==0)
						fullPartitionAccess = false;
				}

				if(fullPartitionAccess){
					newPartitionAttributes.put(p, attrs);
				}
				else{
					//int newPartition = partitionAttributes.size();
					List<Integer> accessedAttrs = new ArrayList<Integer>();
					List<Integer> nonAccessedAttrs = new ArrayList<Integer>();
					for(int a: attrs){
						if(query[a]==1)
							accessedAttrs.add(a);
						else
							nonAccessedAttrs.add(a);
					}
					if(nonAccessedAttrs.size() > 0)
						newPartitionAttributes.put(p, nonAccessedAttrs);
					if(accessedAttrs.size() > 0)
						newPartitionAttributes.put(partitionCount++, accessedAttrs);
				}
			}
			
			//System.out.println(newPartitionAttributes);
			
			partitionAttributes = newPartitionAttributes;
		}
		
		
		// 3. we end up with a set of attribute sets; all attributes in 
		//	  an attribute set are always accessed together.
		int[][] primaryPartitions = new int[partitionAttributes.size()][];
		int i=0;
		for(int p: partitionAttributes.keySet()){
			List<Integer> attrs = partitionAttributes.get(p);
			primaryPartitions[i] = new int[attrs.size()];
			for(int j=0;j<attrs.size();j++)
				primaryPartitions[i][j] = attrs.get(j);
			i++;
		}
		
		return primaryPartitions;
	}
	
	// merge candidate partitions which result in smaller workload costs.
	private List<BigInteger> mergeCandidates(int[][] candidatePartitions){
		Set<BigInteger> mergePartitions = new HashSet<BigInteger>();
		doMerge(mergePartitions, candidatePartitions, BigInteger.valueOf(0), 0);
		return new ArrayList<BigInteger>(mergePartitions);
	}
	
	private void doMerge(Set<BigInteger> mergedPartitions, int[][] candidatePartitions, BigInteger taken, int index){
		// 1. compute the cost of workload on every candidate partition Pi^n obtained by 
		//	  merging n primary partitions (P1^1, ..., Pn^1), for n varying from 2 to |P|
		if(index < candidatePartitions.length){
			doMerge(mergedPartitions, candidatePartitions, taken.setBit(index), index+1);
			doMerge(mergedPartitions, candidatePartitions, taken, index+1);
		}
		else if(taken.bitCount()!=0){
			//System.out.println(taken.toString());
            List<Integer> mergedPartition = new ArrayList<Integer>();
            List<Integer> mergedPartitionIds = new ArrayList<Integer>();

            for(int i=0;i<candidatePartitions.length;i++){
                if(taken.testBit(i)){
                    mergedPartitionIds.add(i);

                    for(int j=0;j<candidatePartitions[i].length;j++)
                        mergedPartition.add(candidatePartitions[i][j]);
                }
            }

            int[] mergedPartitionArr = ArrayUtils.toArrayInteger(mergedPartition);

            int[][] individualPartitions = new int[mergedPartitionIds.size()][];
            int i = 0;
            for(int pid: mergedPartitionIds) {
                individualPartitions[i++] = candidatePartitions[pid];
            }

            double totalIndividualPartitionCosts = costCalculator.getPartitionsCost(individualPartitions);
            double mergedPartitioningCost = costCalculator.getPartitionsCost(new int[][] {mergedPartitionArr});
			
			
			// 2. if the cost of the candidate partition is greater than or equal to the sum of
			//	  the individual costs of the partitions, then this candidate partition can be
			//	  discarded (replaced by the primary partitions), otherwise it is kept.
            if(mergedPartitioningCost >= totalIndividualPartitionCosts){

                for(int pid: mergedPartitionIds)
					mergedPartitions.add(BigInteger.valueOf(0).setBit(pid));
			}
			else
				mergedPartitions.add(taken);
		}
	}
	
	// enumerate return the best possible combination of partitions.
	private void generateLayout(List<BigInteger> candidatePartitions, int[][] partitionAttributes){
		BigInteger validLayout = BigInteger.valueOf(0);
		for(int i=0;i<partitionAttributes.length;i++)
			for(int j=0;j<partitionAttributes[i].length;j++)
				validLayout = validLayout.setBit(partitionAttributes[i][j]);
		
		doGenerate(candidatePartitions, partitionAttributes, BigInteger.valueOf(0), validLayout, 0);
	}
	
	private void doGenerate(List<BigInteger> candidatePartitions, int[][] partitionAttributes, BigInteger taken, BigInteger validLayout, int index){
		// 1. generate the set of all valid layouts (covering but non-overlapping set of partitions)
		if(index < candidatePartitions.size()){
			doGenerate(candidatePartitions, partitionAttributes, taken.setBit(index), validLayout, index+1);
			doGenerate(candidatePartitions, partitionAttributes, taken, validLayout, index+1);
		}
		else{
			BigInteger attributeBitmap = BigInteger.valueOf(0);
			List<int[]> partitions = new ArrayList<int[]>(); 
			for(int i=0;i<candidatePartitions.size();i++){
				if(taken.testBit(i)){
					// non-disjoint?
					for(int p: ArrayUtils.bigIntToArray(candidatePartitions.get(i))){
						BigInteger pBigInt = ArrayUtils.arrayToBigInt(partitionAttributes[p]);
						if(attributeBitmap.and(pBigInt).bitCount() != 0)
							return;
						attributeBitmap = attributeBitmap.or(pBigInt);
						partitions.add(partitionAttributes[p]);
					}
				}
			}
			// not complete?
			if(attributeBitmap.xor(validLayout).bitCount() != 0)
				return;
			
			// 2. evaluate the cost of each valid layout
			double newCost = costCalculator.getPartitionsCost(ArrayUtils.toArrayList(partitions));
			if(newCost < minCost){
				// 3. discard all layouts but the one yielding the lowest cost.
				int[][] partitionsArr = new int[partitions.size()][];
				for(int i=0;i<partitions.size();i++)
					partitionsArr[i] = partitions.get(i);
				bestLayout = partitionsArr;
				minCost = newCost;
			}
		}
	}
	
	private List<int[][]> mergeAcrossLayouts(List<int[][]> layouts, int maxIdx){		
		double maxSaving = 0;
		int _i=-1,_j=-1,_x=-1,_y=-1;
		for(int i=0;i<maxIdx;i++){
			for(int j=0;j<maxIdx;j++){
				if(i!=j){
					// try combining every pair of partitions in layouts i and j
					int[][] layout_i = layouts.get(i);
					int[][] layout_j = layouts.get(j);
					for(int x=0;x<layout_i.length;x++){
						for(int y=0;y<layout_j.length;y++){
                            
                            double c1 = costCalculator.getPartitionsCost(
                                    new int[][] {layout_i[x], layout_j[y]} );	// individual cost
                            double c2 = costCalculator.getPartitionsCost(
                                    new int[][] {ArrayUtils.concatenate(layout_i[x], layout_j[y])} );	// combined cost
							
                            if((c1-c2) > maxSaving){
								maxSaving = c1-c2;
								_i=i;_j=j;_x=x;_y=y;
							}
						}
					}
				}
			}
		}
		
		if(maxSaving > 0){	// reduction possible?
			int[][] layout_i = layouts.get(_i);
			int[][] layout_j = layouts.get(_j);
			
			
			int[][] layout_i_prime;
			if(layout_i.length==1)
				layout_i_prime = new int[][]{};
			else{
				layout_i_prime = new int[layout_i.length-1][];
				for(int x=0;x<layout_i.length;x++){
					if(x == _x)
						continue;
					else if(x > _x)
						layout_i_prime[x-1] = layout_i[x];
					else
						layout_i_prime[x] = layout_i[x];
				}
			}
			
			int[][] layout_j_prime;
			if(layout_j.length==1)
				layout_j_prime = new int[][]{};
			else{
				layout_j_prime = new int[layout_j.length-1][];
				for(int y=0;y<layout_j.length;y++){
					if(y == _y)
						continue;
					else if(y > _y)
						layout_j_prime[y-1] = layout_j[y];
					else
						layout_j_prime[y] = layout_j[y];
				}
			}
			
			layouts.set(_i, layout_i_prime);
			layouts.set(_j, layout_j_prime);
			int[][] newLayout = new int[1][];
			newLayout[0] = ArrayUtils.concatenate(layout_i[_x], layout_j[_y]);
			layouts.add(newLayout);
			
			return mergeAcrossLayouts(layouts, maxIdx);
		}
		else
			return layouts;
	}
}
