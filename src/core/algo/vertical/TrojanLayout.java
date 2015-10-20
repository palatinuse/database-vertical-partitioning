package core.algo.vertical;

import core.metrics.PartitioningCostCalculator;
import core.metrics.PartitioningProfiler;
import core.costmodels.CostModel;
import core.utils.PartitioningUtils;
import db.schema.BenchmarkTables;
import db.schema.entity.*;
import db.schema.entity.Workload.SimplifiedWorkload;
import db.schema.utils.WorkloadUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trojan Data Layouts: Right Shoes for a Running Elephant Alekh Jindal,
 * Jorge-Arnulfo Quiane-Ruiz, Jens Dittrich
 * 
 * ACM SOCC, 2011
 * 
 * @author alekh, Endre Palatinus
 * 
 */
public class TrojanLayout extends AbstractPartitioningAlgorithm {

    /** HACK: value used instead on 1 to avoid a bug in case of log(1)==>0 for interestingness. */
	public static double maxAttributeCost = 0.999999; 
	
	protected GroupInterestingness intg;

    /** Pruning thresholds used for the per-replica attribute partitionings. */
	protected double[] pruningThresholds;
    /** Pruning threshold used for the query partitioning. */
	protected double pruningThreshold;

	protected int replicationFactor;
	protected int[][] perReplicaPartitioning;

	protected Workload.SimplifiedWorkload originalW;

    /* Partitioning of the queries across the replicas (~query routing). */
	protected int[] queryGrouping;

	public TrojanLayout(AlgorithmConfig config) {
		super(config);
		type = Algo.TROJAN;
		intg = new GroupInterestingness();
	}

    @Override
	public void doPartition() {

	  	if (replicationFactor > 1) {
            // do query grouping
            rotateUsageM();
            queryGrouping = doTrojanLayoutPartition()[replicationFactor - 1]; // _TODO how come is the query grouping always the last replica's result?
            revertUsageM();
        } else {
            // all queries routed to the only replica
            queryGrouping = new int[w.queryCount];
        }

        perReplicaPartitioning = new int[replicationFactor][];
        int replica = 0;

        // compute column groups for each query group
        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {

            // skip empty query groups
            if (queryGroup.length == 0) {
                continue;
            }

            /* Attributes referenced by the given query group. */
            int[] refAttrs = WorkloadUtils.getReferencedAttributes(w.usageMatrix, queryGroup);

            /* The partitioning of the referenced subset of attributes for the given query group. */
            int[] subsetPartitioning;
            if (queryGroup.length == 1) {
                // trivial: all referenced attributes should form a single partition
                subsetPartitioning = new int[refAttrs.length];
            } else {
                // again invoke Trojan Layout algorithm to do attribute partitioning
                Table reducedTable = BenchmarkTables.partialTable(t, refAttrs, queryGroup);
                AlgorithmConfig newConfig = config.clone();
                // _TODO this may be problematic due to setting the table or sg. of other configs as well
                newConfig.setTable(reducedTable);
                TrojanLayout tl = (TrojanLayout) AbstractAlgorithm.getAlgo(Algo.TROJAN, newConfig);
                tl.setPruningThreshold(pruningThresholds[replica]);
                tl.setReplicationFactor(1);
                subsetPartitioning = tl.getBestPartitioning(tl.doTrojanLayoutPartition());
            }

            /* The partitioning that takes into account the unreferenced attributes as well
            (puts them into a separate partition). */
            int[] replicaPartitioning = new int[w.attributeCount];
            for (int i = 0; i < refAttrs.length; i++) {
                /* In case there are unreferenced attributes, they go to partition 0,
                 and we simply shift the other partition id's by incrementing them. */
                if (refAttrs.length < w.attributeCount)
                    replicaPartitioning[refAttrs[i]] = subsetPartitioning[i] + 1;
                else
                    replicaPartitioning[refAttrs[i]] = subsetPartitioning[i];
            }

            /* Hack: fix empty partitions bug with consecutive partition IDs */
            perReplicaPartitioning[replica++] = PartitioningUtils.consecutivePartitionIds(replicaPartitioning);
        }

        /* If we have only one replica, than the partitioning can be defined for Trojan layout as well. */
        if (replicationFactor == 1) {
            partitioning = perReplicaPartitioning[0];
        }
	}

	/**
	 * Redundant bytes read in the presence of indexes
	 *  
	 * 
	 * @param workload
	 * @return
	 */
	public int getRedundantBytesRead(Workload workload) {
		int totalRedundantBytes = 0;
		int replica = 0;
		for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
			if (queryGroup.length == 0) {
				continue;
			}
			
			markIndex(workload, queryGroup);
			totalRedundantBytes += profiler.redundantBytesReadPerRow(perReplicaPartitioning[replica++],
                    queryGroup/*, workload*/);
		}
		return totalRedundantBytes;
	}

	/**
	 * Mark one of the attributes as the index attribute
	 * 
	 *   given a set of queries which are mapped to a data block replica,
	 *   we mark one of the attributes as the index attribute i.e. we can
	 *   build a clustered index on this attribute.
	 *   
	 *   we mark the index attribute as follows: 
	 *    - assume there is an index on the attribute
	 *    - calculate the number of rows touched (using the index) over all queries
	 *    - pick the attribute which allows us to touch minimum number of rows 
	 * 
	 * @param w the workload
	 * @param replicaQueries the queries routed to a given replica
	 */
	private void markIndex(Workload w, int[] replicaQueries) {
		List<Query> allQueries = w.queries;

		// get total cardinality of rangeCondition attributes
		Map<Attribute, Long> attributeCardinality = new HashMap<Attribute, Long>();
		for (int q : replicaQueries) {
            if (allQueries.get(q) instanceof RangeQuery) {
                Range r = ((RangeQuery) allQueries.get(q)).getRangeCondition();
                Attribute a = r.a;
                long count = w.getNumberOfRows() - r.count();
                if (!attributeCardinality.containsKey(a))
                    attributeCardinality.put(a, count);
                else
                    attributeCardinality.put(a, attributeCardinality.get(a) + count);
            }
		}

		// get the attribute with minimum cardinality
		long minCardinality = Long.MAX_VALUE;
		Attribute minCardinalityAttribute = null;
		for (Attribute a : attributeCardinality.keySet()) {
			if (attributeCardinality.get(a) < minCardinality){
				minCardinalityAttribute = a;
                minCardinality = attributeCardinality.get(a);
            }
		}

		// mark the index flag for ranges having minimum cardinality attribute
		for (int q : replicaQueries) {
			if (allQueries.get(q) instanceof RangeQuery) {
                Range r = ((RangeQuery)allQueries.get(q)).getRangeCondition();
                r.isIndexed = r.a.equals(minCardinalityAttribute);
            }
		}
	}

	protected void rotateUsageM() {
		originalW = (SimplifiedWorkload) w.clone();

		w.usageMatrix = PartitioningUtils.transposeMatrix(w.usageMatrix);
		w.usageMatrix = PartitioningUtils.getNonNullVectors(w.usageMatrix);
		w.attributeSizes = w.queryWeights;

		w.queryCount = w.usageMatrix.length;
		try {
			w.attributeCount = w.usageMatrix[0].length;
		} catch (Exception ex) {
			w.attributeCount = 0;
		}
	}

	protected void revertUsageM() {
		w = originalW;
        profiler = new PartitioningProfiler(w);

        config.setW(w);
        costCalculator = PartitioningCostCalculator.create(config);
	}

	protected int[][] doTrojanLayoutPartition() {
		intg.setup();
		Map<Long, Double> subsets = enumerate(intg, w.attributeCount, pruningThreshold);
		profiler.candidateSetSize = subsets.size();

		BBKnapsackSolver cgrp = new BBKnapsackSolver(subsets);
		cgrp.bbKnapsack(0, 0, 0, 0, BigInteger.valueOf(0));

		profiler.numberOfIterations = cgrp.count;
		BigInteger[] solution = cgrp.maxBenefitItemVector;
		return cgrp.getItemPartitions(solution);
	}

	protected int[] getBestPartitioning(int[][] partitionings) {
		double minCost = Double.MAX_VALUE;
		int[] bestPartitioning = null;
		for (int[] partitioning : partitionings) {
			if (!checkPartitioning(partitioning))
				continue;
			double cost = costCalculator.getPartitioningCost(PartitioningUtils.consecutivePartitionIds(partitioning));
			if (cost < minCost) {
				bestPartitioning = partitioning;
				minCost = cost;
			}
		}

		if (bestPartitioning == null) // set default partitioning to row
			bestPartitioning = new int[w.attributeCount];

		return bestPartitioning;
	}

	protected boolean checkPartitioning(int[] partitioning) {
		boolean flag = true;
		for (int p : partitioning)
			if (p < 0)
				flag = false;
		return flag;
	}

	protected Map<Long, Double> enumerate(GroupInterestingness intg, int a, double t) {
		List<Long> groups = new ArrayList<Long>();
		long N = (long) 1 << a;
		for (long i = 1; i < N; i++) {
			double interestingness = intg.getInterestingness(i);
			/*if(i==48 || i==6 || i==54)
			    System.out.println(i+" : "+interestingness);*/
			if (interestingness >= t) {
				groups.add(i);
			}
		}
		Map<Long, Double> groupInterestingess = new HashMap<Long, Double>();
		for (Long g : groups) {
			// System.out.println(g+"\t"+intg.getInterestingness(g));
			// if(Long.bitCount(g) > 1) /* Hack to enumerate over only
			// multi-attribute column groups */
			groupInterestingess.put(g, intg.getInterestingness(g));
		}
		// groupInterestingess =
		// getUniquelyInterestingGroups(groupInterestingess); // comment out
		// this line for disabling the interestingness filter

		return groupInterestingess;
	}

    //----- Getter-setter functions -----//
    
    @Override
    public int[] getPartitioning() {
        if (replicationFactor != 1) {
            throw new UnsupportedOperationException("Partitioning is not defined for replication factor = " + replicationFactor);
        } else {
            return partitioning;
        }
    }

    public void setPruningThresholds(double[] pruningThresholds) {
        this.pruningThresholds = pruningThresholds;
    }

    public void setPruningThreshold(double pruningThreshold) {
        this.pruningThreshold = pruningThreshold;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public int[][] getPerReplicaPartitioning() {
        return perReplicaPartitioning;
    }

	@Override
	public TIntObjectHashMap<TIntHashSet> getPartitions() {
		if (partitioning != null) {
			return PartitioningUtils.getPartitioningMap(partitioning);
		} else {
			return null;
		}
	}

    //----- End of getter-setter functions -----//

    //----- Metrics- and cost calculation part. -----//

    public void setMetricCalculators(PartitioningCostCalculator costCalculator, PartitioningProfiler profiler) {
        this.costCalculator = costCalculator;
        this.profiler = profiler;
    }

    //@Override
    public int getRedundantBytesRead() {
        int totalRedundantBytes = 0;
        int replica = 0;
        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
            if (queryGroup.length == 0) {
                continue;
            }

            totalRedundantBytes += profiler.redundantBytesReadPerRow(perReplicaPartitioning[replica++],
                    queryGroup);
        }

        return totalRedundantBytes;
    }

    //@Override
    public double getEstimatedCost() {

        double cost = 0.0;
        int replica = 0;

        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
            if (queryGroup.length == 0) {
                continue;
            }

            cost += costCalculator.getPartitionsCost(PartitioningUtils.
                    getPartitions(perReplicaPartitioning[replica++]), queryGroup);
        }

        return cost;
    }

    //@Override
    public double getEstimatedSeekCost() {
        double cost = 0.0;
        int replica = 0;

        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
            if (queryGroup.length == 0) {
                continue;
            }

            cost += costCalculator.getPartitionsCosts(PartitioningUtils.
                    getPartitions(perReplicaPartitioning[replica++]), queryGroup)[CostModel.SEEK];
        }

        return cost;
    }

    //@Override
    public double getEstimatedScanCost() {
        double cost = 0.0;
        int replica = 0;

        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
            if (queryGroup.length == 0) {
                continue;
            }

            cost += costCalculator.getPartitionsCosts(PartitioningUtils.
                    getPartitions(perReplicaPartitioning[replica++]), queryGroup)[CostModel.SCAN];
        }

        return cost;
    }

    //@Override
    public int getAttributeJoins() {
        int totalAttributeJoins = 0;
        int replica = 0;
        for (int[] queryGroup : PartitioningUtils.getPartitions(queryGrouping)) {
            if (queryGroup.length == 0) {
                continue;
            }

            totalAttributeJoins += profiler.attributeJoinsPerRow(perReplicaPartitioning[replica++], queryGroup);
        }
        return totalAttributeJoins;
    }

    /**
     * Method for calculating the cost of transforming the current layout to the newly calculated partitioning.
     * @param sourcePartitioning The current layout.
     * @param targetPartitioning The newly calculated layout.
     * @return The cost of reading the current layout and writing the target layout.
     */
    public double layoutCreationCost(int[][] sourcePartitioning, int[][] targetPartitioning) {
        double cost = 0.0;

        assert sourcePartitioning.length == targetPartitioning.length;
        
        for (int replica = 0; replica < targetPartitioning.length; replica++) {
            cost += costCalculator.layoutCreationCost(sourcePartitioning[replica], targetPartitioning[replica]);
        }

        return cost;
    }

    //----- End of metrics and cost calculation part. -----//

	/**
	 * Branch and Bound Knapsack solver
	 * 
	 * @author alekh
	 * 
	 */
	protected class BBKnapsackSolver {

		int W;
		long[] weights;
		double[] b;
		long count = 0;

		double maxBenefit[];
		BigInteger maxBenefitItemVector[];

		public BBKnapsackSolver(Map<Long, Double> weightBenefits) {
			int maxGroups = w.attributeCount;

			maxBenefit = new double[maxGroups];
			maxBenefitItemVector = new BigInteger[maxGroups];

			for (int i = 0; i < maxGroups; i++)
				W = 1 | (W << 1);

			long[] weights = new long[weightBenefits.size()];
			double[] benefits = new double[weightBenefits.size()];
			int i = 0;
			for (Long s : weightBenefits.keySet()) {
				weights[i] = s;
				benefits[i] = weightBenefits.get(s);
				i++;
			}
			this.weights = weights;
			b = benefits;
		}

		/**
		 * Branch and bound knapsack algorithm We branch over all possible
		 * combinations in the knapsack and bound based on the following
		 * condition: The bit vectors of the column group in the knapsack must
		 * have mutual bitwise AND equal to 0.
		 * 
		 * @param i
		 *            - the index of the item being considered for addition in
		 *            to the knapsack
		 * @param totalBenefit
		 *            - total benefit of the column groups in the knapsack
		 * @param totalWeight
		 *            - total weights of the column groups in the knapsack
		 * @param bitVector
		 *            - which attributes (across all column groups) are
		 *            currently in the knapsack
		 * @param itemVector
		 *            - which items (column groups) are currently in the
		 *            knapsack
		 */
		public void bbKnapsack(int i, double totalBenefit, long totalWeight, long bitVector, BigInteger itemVector) {

			// System.out.println(i+","+count);

			// check if end of item list reached
			if (i == weights.length) {
				count++;
				int idx = itemVector.bitCount() - 1;
				if (totalWeight < W)
					idx += 1;

				if (idx >= maxBenefit.length) {
					System.out.println("Problem!");
				}

				if (idx >= 0 && totalBenefit > maxBenefit[idx]) {
					maxBenefit[idx] = totalBenefit;
					maxBenefitItemVector[idx] = itemVector;
				}
				// System.out.println("Benefit: "+totalBenefit+", Bit Vector: "+vStr(bitVector)+", Item Vector: "+vStr(itemVector));
				return;
			}

			// System.out.println(i+","+totalBenefit+","+totalWeight+","+bitVector);
			bbKnapsack(i + 1, totalBenefit, totalWeight, bitVector, itemVector); // out
			if (totalWeight + weights[i] <= W && (bitVector & weights[i]) == 0)
				bbKnapsack(i + 1, totalBenefit + b[i], totalWeight + weights[i], (bitVector | weights[i]),
						itemVector.or(BigInteger.valueOf(1).shiftLeft(i))); // in
		}

		public int[][] getItemPartitions(BigInteger[] maxBenefitItemVector) {
			int[][] partitions = new int[maxBenefitItemVector.length][maxBenefitItemVector.length];
			for (int j = 0; j < maxBenefitItemVector.length; j++) {
				// System.out.println("Number of Partitions: "+(j+1));
				int[] partition = new int[maxBenefitItemVector.length];
				int p = -1;
				if (maxBenefitItemVector[j] == null) {
					for (int i = 0; i < partition.length; i++)
						partition[i] = p;
					partitions[j] = partition;
					continue;
				}
				p++;
				for (int i = 0; maxBenefitItemVector[j].compareTo(BigInteger.valueOf(0)) > 0; maxBenefitItemVector[j] = maxBenefitItemVector[j]
						.shiftRight(1), i++) {
					if (maxBenefitItemVector[j].testBit(0)) {
						p++;
						partition = setPartitionId(weights[i], maxBenefitItemVector.length, partition, p);
						// System.out.print(ColumnGrouping.vStr(w[i],
						// maxBenefitItemVector.length)+":"+b[i]+",");
					}
				}
				partitions[j] = partition;
				// System.out.println();
			}
			return partitions;
		}

		protected int[] setPartitionId(long v, int size, int[] partition, int p) {
			for (int i = 0; size > 0; v = v >> 1, size--, i++) {
				if ((v & 1) == 1) {
					partition[i] = p;
				}
			}
			return partition;
		}
	}

	/**
	 * The interestingness measure for grouping
	 * 
	 * @author alekh
	 * 
	 */
	protected class GroupInterestingness {

		public double[] sel;
		public double[] queryFootprint;
		public double totalQueryFootprint;

		protected void setup() {
			sel = new double[w.queryCount];
			queryFootprint = new double[w.queryCount];
			for (int i = 0; i < w.queryCount; i++) {
				sel[i] = 1;
				double f = getQueryFootprint(i);
				queryFootprint[i] = f;
				totalQueryFootprint += f;
			}

			// print attribute importances
			// System.out.println("Printing attribute importances");
			// for(int i=0; i<attributeCount; i++)
			// System.out.println("Attribute:"+i+", RIx=1:"+getAttributeCost(i,
			// 1));
		}

		protected double getAttributeBytes(int attribute, int query, Object optimizer) {
			// TODO:compute the access from the optimizer
			return w.numRows * w.attributeSizes[attribute] * sel[query];
        }

		protected double getQueryFootprint(int q) {
			double F = 0.0;
			for (int i = 0; i < w.usageMatrix[q].length; i++)
				if (w.usageMatrix[q][i] > 0)
					F += getAttributeBytes(i, q, null);
			return F;
		}

		protected double getAttributeCost(int attribute, int x) {
			double C_A = 0.0;
			for (int i = 0; i < w.queryCount; i++) {
				if (x == 1)
					C_A += queryFootprint[i] * w.usageMatrix[i][attribute];
				else
					C_A += queryFootprint[i] * (1 - w.usageMatrix[i][attribute]);
			}
			if(C_A == totalQueryFootprint)
				return maxAttributeCost;		// hack
			else
				return C_A / totalQueryFootprint;
		}

		protected double getAttributePairCost(int attribute1, int attribute2, int x, int y) {
			double C_A = 0.0;
			for (int i = 0; i < w.queryCount; i++) {
				int usage1, usage2;
				if (x == 1)
					usage1 = w.usageMatrix[i][attribute1];
				else
					usage1 = 1 - w.usageMatrix[i][attribute1];
				if (y == 1)
					usage2 = w.usageMatrix[i][attribute2];
				else
					usage2 = 1 - w.usageMatrix[i][attribute2];
				C_A += queryFootprint[i] * usage1 * usage2;
			}
			if(C_A == totalQueryFootprint)
				return maxAttributeCost;		// hack
			else
				return C_A / totalQueryFootprint;
		}

		protected double getEntropy(int attribute) {
			double ent = 0.0;
			double p = getAttributeCost(attribute, 0);
			if (p > 0)
				ent += -p * Math.log(p);
			p = getAttributeCost(attribute, 1);
			if (p > 0)
				ent += -p * Math.log(p);
			return ent;
		}

		protected double getMI(int attribute1, int attribute2) {
			double mi = 0.0;
			double p = getAttributePairCost(attribute1, attribute2, 0, 0);
			if (p > 0)
				mi += p * Math.log(p / getAttributeCost(attribute1, 0) / getAttributeCost(attribute2, 0));
			p = getAttributePairCost(attribute1, attribute2, 0, 1);
			if (p > 0)
				mi += p * Math.log(p / getAttributeCost(attribute1, 0) / getAttributeCost(attribute2, 1));
			p = getAttributePairCost(attribute1, attribute2, 1, 0);
			if (p > 0)
				mi += p * Math.log(p / getAttributeCost(attribute1, 1) / getAttributeCost(attribute2, 0));
			p = getAttributePairCost(attribute1, attribute2, 1, 1);
			if (p > 0)
				mi += p * Math.log(p / getAttributeCost(attribute1, 1) / getAttributeCost(attribute2, 1));
			return mi;
		}

		protected double getNormMI(int attribute1, int attribute2) {
			double ent1 = getEntropy(attribute1);
			double ent2 = getEntropy(attribute2);
			double minEnt = ent1 < ent2 ? ent1 : ent2;
			if (getMI(attribute1, attribute2) == 0)
				return 0;
			else if (minEnt == 0)
				return 1;
			else
				return getMI(attribute1, attribute2) / minEnt;
		}

		protected double getNormInvMI(int attribute1, int attribute2) {
			double ent1 = getEntropy(attribute1);
			double ent2 = getEntropy(attribute2);
			double minEnt = ent1 < ent2 ? ent1 : ent2;
			if (getMI(attribute1, attribute2) == 0)
				return 0;
			else if (minEnt == 0)
				return 1;
			else
				return (minEnt - getMI(attribute1, attribute2)) / minEnt;
		}

		protected double getInterestingness(int[] group) {

			double interestingness = 0.0;
			if (group.length == 1) {
				int i = group[0], count = 0;
				double totalMI = 0;
				for (int j = 0; j < w.attributeCount; j++) {
					if (i == j)
						continue;
					totalMI += getNormInvMI(i, j);
					count++;
				}
				if (count == 0)
					interestingness = 1;
				else
					interestingness = totalMI / count;
			} else {
				int count = 0;
				for (int i = 0; i < group.length - 1; i++) {
					for (int j = i + 1; j < group.length; j++) {
						interestingness += getNormMI(group[i], group[j]);
						count++;
					}
				}
				interestingness /= count;
			}
			return interestingness;
		}

		public double getInterestingness(long group) {
			List<Integer> groupItems = new ArrayList<Integer>();
			for (int i = 0; group > 0; group = group >> 1, i++) {
				if ((group & 1) == 1)
					groupItems.add(i);
			}
			int[] groupArray = new int[groupItems.size()];
			for (int i = 0; i < groupArray.length; i++)
				groupArray[i] = groupItems.get(i);
			return getInterestingness(groupArray);
		}
	}
}
