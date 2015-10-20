package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import core.costmodels.HDDCostModel;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Class for cost computations for algorithms producing possibly overlapping partitions.
 * 
 * @author Endre Palatinus
 */
public final class HDDPartitionsCostCalculator extends PartitionsCostCalculator {

	private CostModel cm;

	public HDDPartitionsCostCalculator(AbstractAlgorithm.AlgorithmConfig config) {
        super(config);

        AbstractAlgorithm.HDDAlgorithmConfig HDDconfig = (AbstractAlgorithm.HDDAlgorithmConfig)config;
		this.cm = HDDconfig.costModel;
	}

    @Override
    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return The calculated cost.
     */
    public double getPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {

        double cost = 0.0;

        TIntIntHashMap partitionRowSize = new TIntIntHashMap(partitions.size());
        for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext();) {
            int p = pit.next();

            int rowSize = 0;
            for (TIntIterator ait = partitions.get(p).iterator(); ait.hasNext();) {
                rowSize += w.attributeSizes[ait.next()];
            }

            partitionRowSize.put(p,rowSize);
        }
        
        for (int q = 0; q < w.queryCount; q++) {
            cost += getPartitionsCostForQuery(partitionRowSize, bestSolutions.get(q));
        }

        return cost;
    }

    @Override
    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     *
     * @param partitions
     *            The map of partitions.
     * @param bestSolutions
     *            The partition IDs of the chosen partitions.
     * @return An array with 2 doubles, the first being the seek- and the second the scan cost.
     */
    public double[] getPartitionsCosts(TIntObjectHashMap<TIntHashSet> partitions, TIntObjectHashMap<TIntHashSet> bestSolutions) {

        double seekCost = 0.0;
        double scanCost = 0.0;

        TIntIntHashMap partitionRowSize = new TIntIntHashMap(partitions.size());
        for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext();) {
            int p = pit.next();

            int rowSize = 0;
            for (TIntIterator ait = partitions.get(p).iterator(); ait.hasNext();) {
                rowSize += w.attributeSizes[ait.next()];
            }

            partitionRowSize.put(p,rowSize);
        }

        for (int q = 0; q < w.queryCount; q++) {
            double[] costs = getPartitionsCostsForQuery(partitionRowSize, bestSolutions.get(q));
            seekCost += costs[CostModel.SEEK];
            scanCost += costs[CostModel.SCAN];
        }

        double[] costs = new double[2];
        costs[CostModel.SEEK] = seekCost;
        costs[CostModel.SCAN] = scanCost;

        return costs;
    }

	/**
     * Method for calculating the I/O cost of an overlapping partitioning for a
     * given query, given the partitions chosen for usage for the query. Note
     * that this method overwrites the value of rowSize and refRowSize according
     * to the partitioning.
     *
     * @param partitionRowSizes
     *            The row sizes of the partitions.
     * @param currentSolution
     *            The partition IDs of the chosen partitions.
     * @return The cost for the given setting.
     */
    protected double getPartitionsCostForQuery(TIntIntHashMap partitionRowSizes, TIntHashSet currentSolution) {

        int referencedPartitionsRowSize = 0;

        for (TIntIterator pit = currentSolution.iterator(); pit.hasNext();) {
            referencedPartitionsRowSize += partitionRowSizes.get(pit.next());
        }

        double cost = 0.0;

        for (TIntIterator pit = currentSolution.iterator(); pit.hasNext();) {
            cost += cm.getCostForPartition(partitionRowSizes.get(pit.next()), referencedPartitionsRowSize);
        }

        return cost;
    }

    /**
     * Method for calculating the I/O cost of an overlapping partitioning for a
     * given query, given the partitions chosen for usage for the query. Note
     * that this method overwrites the value of rowSize and refRowSize according
     * to the partitioning.
     *
     * @param partitionRowSize
     *            The row sizes of the partitions.
     * @param currentSolution
     *            The partition IDs of the chosen partitions.
     * @return The cost for the given setting.
     */
    protected double[] getPartitionsCostsForQuery(TIntIntHashMap partitionRowSize, TIntHashSet currentSolution) {

        int referencedPartitionsRowSize = 0;

        for (TIntIterator pit = currentSolution.iterator(); pit.hasNext();) {
            referencedPartitionsRowSize += partitionRowSize.get(pit.next());
        }

        double seekCost = 0.0;
        double scanCost = 0.0;

        for (TIntIterator pit = currentSolution.iterator(); pit.hasNext();) {
            double[] costs = cm.getCostsForPartition(partitionRowSize.get(pit.next()), referencedPartitionsRowSize);
            seekCost += costs[CostModel.SEEK];
            scanCost += costs[CostModel.SCAN];
        }

        double[] costs = new double[2];
        costs[CostModel.SEEK] = seekCost;
        costs[CostModel.SCAN] = scanCost;

        return costs;
    }

    @Override
	/**
	 * Method for calculating the cost of a possibly overlapping partitioning.
	 * Note that this method overwrites the value of rowSize and refRowSize
	 * according to the partitioning, and stores the best partitions to be used
	 * for executing a query.
	 * 
	 * @param partitions
	 *            The map of partitions.
	 * @return The calculated cost.
	 */
	public double findPartitionsCost(TIntObjectHashMap<TIntHashSet> partitions) {

		workload.setBestSolutions(new TIntObjectHashMap<TIntHashSet>());

		double cost = 0.0;

        TIntIntHashMap partitionRowSize = new TIntIntHashMap(partitions.size());
        for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext();) {
            int p = pit.next();

            int rowSize = 0;
            for (TIntIterator ait = partitions.get(p).iterator(); ait.hasNext();) {
                rowSize += w.attributeSizes[ait.next()];
            }

            partitionRowSize.put(p,rowSize);
        }

		for (int q = 0; q < w.queryCount; q++) {

			// the attributes referenced by the query
			TIntArrayList queryAccessSet = new TIntArrayList(w.attributeCount);

			for (int a = 0; a < w.attributeCount; a++) {
				if (w.usageMatrix[q][a] == 1) {
					queryAccessSet.add(a);
				}
			}

			// which partitions can be used for a given referenced attribute
			TIntObjectHashMap<TIntHashSet> candidatePartitions = new TIntObjectHashMap<TIntHashSet>();

			for (TIntIterator ait = queryAccessSet.iterator(); ait.hasNext();) {
				int a = ait.next();
				candidatePartitions.put(a, new TIntHashSet(w.attributeCount));

				for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext();) {
					int p = pit.next();

					if (partitions.get(p).contains(a)) {
						candidatePartitions.get(a).add(p);
					}
				}
			}

			HDDPartitionSelectionPlanSolver ocm = new HDDPartitionSelectionPlanSolver(partitions, queryAccessSet, candidatePartitions, partitionRowSize);
			ocm.findBestPartitions(0, new TIntHashSet(w.attributeCount), new TIntHashSet(w.attributeCount));

			cost += ocm.getBestCost();
			workload.getBestSolutions().put(q, ocm.getBestSolution());
		}

		return cost;
	}

    @Override
    /**
     * Method for calculating the cost of transforming the current layout to the newly calculated partitioning.
     * @param sourcePartitions The current layout.
     * @param targetPartitions The newly calculated layout.
     * @return The cost of reading the current layout and writing the target layout.
     */
    public double layoutCreationCost(TIntObjectHashMap<TIntHashSet> sourcePartitions,
                                     TIntObjectHashMap<TIntHashSet> targetPartitions) {

        double cost = 0.0;

        // read source partitioning

        /* The row sizes of the partitions. */
        TIntIntHashMap partitionRowSize = new TIntIntHashMap();
        /* The sum of the row sizes of the partitions. */
        int totalPartitionsRowSize = 0;

        for (int p : sourcePartitions.keys()) {

            int partRowSize = 0;
            for (int a : sourcePartitions.get(p).toArray()) {
                partRowSize += w.attributeSizes[a];
            }
            partitionRowSize.put(p, partRowSize);
            totalPartitionsRowSize += partitionRowSize.get(p);
        }

        for (int p : sourcePartitions.keys()) {
            cost += cm.getCostForPartition(partitionRowSize.get(p), totalPartitionsRowSize);
        }


        // write target partitioning

        partitionRowSize = new TIntIntHashMap();
        totalPartitionsRowSize = 0;

        for (int p : targetPartitions.keys()) {

            int partRowSize = 0;
            for (int a : targetPartitions.get(p).toArray()) {
                partRowSize += w.attributeSizes[a];
            }
            partitionRowSize.put(p, partRowSize);
            totalPartitionsRowSize += partitionRowSize.get(p);
        }

        // HACK: only suitable for HDDCostModel
        HDDCostModel HDDcm = (HDDCostModel)cm;
        double oldBW_disk = HDDcm.getReadDiskBW();
        HDDcm.setReadDiskBW(HDDcm.getWriteDiskBW());

        for (int p : targetPartitions.keys()) {
            cost += cm.getCostForPartition(partitionRowSize.get(p), totalPartitionsRowSize);
        }

        HDDcm.setReadDiskBW(oldBW_disk);

        return cost;
    }

	/**
	 * Class for finding the best subset of overlapping partitions to be used
	 * for a given query.
	 */
	private class HDDPartitionSelectionPlanSolver {

		private TIntObjectHashMap<TIntHashSet> partitions;
		private TIntArrayList queryAccessSet;
		private TIntObjectHashMap<TIntHashSet> candidatePartitions;
        private TIntIntHashMap partitionRowSizes;

		private double bestCost;
		private TIntHashSet bestSolution;

        /**
         * Constructor
         * @param partitions The current partitions of the table.
         * @param queryAccessSet The set of attributes referenced by the query.
         * @param candidatePartitions The partitions that contain attributes referenced by the actual query.
         * @param partitionRowSizes The row sizes of the partitions, needed by the cost calculation.
         */
		public HDDPartitionSelectionPlanSolver(TIntObjectHashMap<TIntHashSet> partitions, TIntArrayList queryAccessSet,
                                               TIntObjectHashMap<TIntHashSet> candidatePartitions, TIntIntHashMap partitionRowSizes) {

			this.partitions = partitions;
			this.queryAccessSet = queryAccessSet;
			this.candidatePartitions = candidatePartitions;
            this.partitionRowSizes = partitionRowSizes;
		}

		/**
		 * Method for finding the best subset of overlapping partitions to be
		 * used for a given query. It enumerates all possible solutions
		 * recursively.
		 * 
		 * It stores the best cost and the best solution in local variables.
		 * 
		 * @param index
		 *            The index of the actual attribute in the queryAccessSet
		 *            for which we try to find a solution.
		 * @param alreadyCovered
		 *            The attributes that are already covered by the current
		 *            partial solution.
		 * @param currentSolution
		 *            The partition IDs chosen.
		 */
		public void findBestPartitions(int index, TIntHashSet alreadyCovered, TIntHashSet currentSolution) {

			if (index == 0) {
				bestCost = Double.MAX_VALUE;
			}

			for (TIntIterator pit = candidatePartitions.get(queryAccessSet.get(index)).iterator(); pit.hasNext();) {
				int p = pit.next();

				TIntHashSet partition = partitions.get(p);
                TIntHashSet intersection = new TIntHashSet(partition);
                intersection.retainAll(queryAccessSet);

				TIntHashSet newCurrentSolution = new TIntHashSet(currentSolution);
				TIntHashSet newAlreadyCovered = new TIntHashSet(alreadyCovered);

				if (!alreadyCovered.containsAll(intersection)) {
					newAlreadyCovered.addAll(intersection);
					newCurrentSolution.add(p);
				}

				/*
				 * the attribute with the last index has just been covered, or
				 * even before that all of them have been covered
				 */
				if (index == queryAccessSet.size() - 1 || newAlreadyCovered.size() == queryAccessSet.size()) {
					
					// cost calculation
					double cost = getPartitionsCostForQuery(partitionRowSizes, newCurrentSolution);

					if (cost < bestCost) {
						bestCost = cost;
						bestSolution = newCurrentSolution;
					}

				} else {
					findBestPartitions(index + 1, newAlreadyCovered, newCurrentSolution);
				}
			}

			if (alreadyCovered.contains(queryAccessSet.get(index))) {
				getPartitionsCostForQuery(partitionRowSizes, currentSolution);
			}
		}

		public double getBestCost() {
			return bestCost;
		}

		public TIntHashSet getBestSolution() {
			return bestSolution;
		}
	}
}
