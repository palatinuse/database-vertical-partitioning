package core.algo.vertical;

import core.utils.CollectionUtils;
import core.utils.PartitioningUtils;
import db.schema.utils.WorkloadUtils;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.HashSet;

/**
 * Implementation of the AutoPart vertical partitioning algorithm from S.
 * Papadomanolakis and A. Ailamaki, SSDBM '04.
 * 
 * @author Endre Palatinus
 * 
 */
public class AutoPart extends AbstractPartitionsAlgorithm {

	/**
	 * The amount of storage available for attribute replication expressed as a
	 * factor of increase in storage requirements.
	 */
	private double replicationFactor = 0.5;

	/** The minimal number of queries that should access a candidate fragment. */
	private int queryExtentThreshold = 1;

	public AutoPart(AlgorithmConfig config) {
		super(config);
		type = Algo.AUTOPART;
	}

	@Override
	public void doPartition() {

        TIntHashSet unReferenced = WorkloadUtils.getNonReferencedAttributes(w.usageMatrix);
        HashSet<TIntHashSet> unRefHashSet = new HashSet<TIntHashSet>();
        unRefHashSet.add(unReferenced);
        int unReferencedSize = getOverlappingPartitionsSize(unRefHashSet);

		/* Atomic fragment selection. */

		HashSet<TIntHashSet> atomicFragments = new HashSet<TIntHashSet>();

		HashSet<TIntHashSet> newFragments = new HashSet<TIntHashSet>();
		HashSet<TIntHashSet> toBeRemovedFragments = new HashSet<TIntHashSet>();

		for (int q = 0; q < w.queryCount; q++) {

			TIntHashSet queryExtent = new TIntHashSet(w.attributeCount);

			for (int a = 0; a < w.attributeCount; a++) {
				if (w.usageMatrix[q][a] == 1) {
					queryExtent.add(a);
				}
			}

			newFragments.clear();
			toBeRemovedFragments.clear();

			for (TIntHashSet fragment : atomicFragments) {

				TIntHashSet intersection = new TIntHashSet(queryExtent);
				intersection.retainAll(fragment);

				if (!intersection.isEmpty()) {

					toBeRemovedFragments.add(fragment);
					TIntHashSet remainder = new TIntHashSet(fragment);
					remainder.removeAll(intersection);

					if (!remainder.isEmpty()) {
						newFragments.add(remainder);
					}

					if (!intersection.isEmpty()) {
						newFragments.add(intersection);
					}

					queryExtent.removeAll(intersection);

					if (queryExtent.isEmpty()) {
						break;
					}
				}

			}

			if (!queryExtent.isEmpty()) {
				newFragments.add(queryExtent);
			}

			atomicFragments.removeAll(toBeRemovedFragments);
			atomicFragments.addAll(newFragments);
		}

		/* Iteration phase */

		/* The partitions in the current solution. */
		HashSet<TIntHashSet> presentSolution = CollectionUtils.deepClone(atomicFragments);
		/*
		 * The fragments selected for inclusion into the solution in the
		 * previous iteration.
		 */
		HashSet<TIntHashSet> selectedFragments_prev = new HashSet<TIntHashSet>();
		/*
		 * The fragments selected for inclusion into the solution in the current
		 * iteration.
		 */
		HashSet<TIntHashSet> selectedFragments_curr = CollectionUtils.deepClone(atomicFragments);
		/*
		 * The fragments that will be considered for inclusion into the solution
		 * in the current iteration.
		 */
		HashSet<TIntHashSet> candidateFragments = new HashSet<TIntHashSet>();

		/* Iteration count. */
		int k = 0;

		boolean stoppingCondition = false;

		while (!stoppingCondition) {

			k++;

			/* composite fragment generation */

			candidateFragments.clear();
			selectedFragments_prev.clear();
			selectedFragments_prev.addAll(selectedFragments_curr);

			for (TIntHashSet CF : selectedFragments_prev) {

				// with atomic fragments
				for (TIntHashSet AF : atomicFragments) {
					TIntHashSet fragment = new TIntHashSet(CF);
					fragment.addAll(AF);

					if (queryExtent(fragment) >= queryExtentThreshold) {
						candidateFragments.add(fragment);
					}
				}

				// with fragments selected in the previous iteration
				if (k > 1) {
					for (TIntHashSet F : selectedFragments_prev) {
						TIntHashSet fragment = new TIntHashSet(CF);
						fragment.addAll(F);

						if (queryExtent(fragment) >= queryExtentThreshold) {
							candidateFragments.add(fragment);
						}
					}
				}

			}

			/* candidate fragment selection */

			selectedFragments_curr.clear();
			boolean solutionFound = true;

			double presentCost = costCalculator
					.findPartitionsCost(PartitioningUtils.getPartitioningMap(presentSolution));
			double bestCost = presentCost;
			HashSet<TIntHashSet> bestSolution = presentSolution;
			TIntHashSet selectedFragment = null;

			while (solutionFound) {

				solutionFound = false;

				for (TIntHashSet candidate : candidateFragments) {

					if (presentSolution.contains(candidate)) {
						continue;
					}

					HashSet<TIntHashSet> newSolution = CollectionUtils.deepClone(presentSolution);
					newSolution = addFragment(newSolution, candidate);

                    if (getOverlappingPartitionsSize(newSolution) + unReferencedSize <= (1 + replicationFactor) * w.rowSize) {

						presentCost = costCalculator.findPartitionsCost(PartitioningUtils
								.getPartitioningMap(newSolution));

						//System.out.println(newSolution + " - " + presentCost + " / " + bestCost);

						if (presentCost < bestCost) {
							bestCost = presentCost;
							bestSolution = newSolution;
							selectedFragment = candidate;

							solutionFound = true;
						}
					}
				}

				if (solutionFound) {
					presentSolution = bestSolution;
					selectedFragments_curr.add(selectedFragment);
					candidateFragments.remove(selectedFragment);
				}
			}

			// update stoppingCondition
			stoppingCondition = selectedFragments_curr.size() == 0;
		}

		profiler.numberOfIterations = k;

		partitions = PartitioningUtils.getPartitioningMap(presentSolution);

		/* pairwise merge phase */

		stoppingCondition = false;

		double bestCost = costCalculator.findPartitionsCost(PartitioningUtils.getPartitioningMap(presentSolution));
		int bestI = 0, bestJ = 0; // the indexes of the to-be merged fragments

		/* just a utility representation of the solution */
		TIntObjectHashMap<TIntHashSet> partitionsMap;

		while (!stoppingCondition) {
			stoppingCondition = true;
			//partitionsMap = CollectionUtils.deepClone(partitions);
            partitionsMap = PartitioningUtils.getPartitioningMap(presentSolution);

			HashSet<TIntHashSet> modifiedSolution = null;

			for (int i = 1; i <= partitionsMap.size(); i++) {
				for (int j = i + 1; j <= partitionsMap.size(); j++) {

					modifiedSolution = new HashSet<TIntHashSet>(presentSolution);
					modifiedSolution.remove(partitionsMap.get(i));
					modifiedSolution.remove(partitionsMap.get(j));
					TIntHashSet mergedIJ = new TIntHashSet(w.attributeCount);
					mergedIJ.addAll(partitionsMap.get(i));
					mergedIJ.addAll(partitionsMap.get(j));
					modifiedSolution.add(mergedIJ);

					double presentCost = costCalculator.findPartitionsCost(PartitioningUtils
							.getPartitioningMap(modifiedSolution));

					if (presentCost < bestCost) {
						bestCost = presentCost;

						bestI = i;
						bestJ = j;

						stoppingCondition = false;
					}
				}
			}

			if (!stoppingCondition) {
				presentSolution.remove(partitionsMap.get(bestI));
				presentSolution.remove(partitionsMap.get(bestJ));
				TIntHashSet mergedIJ = new TIntHashSet(w.attributeCount);
				mergedIJ.addAll(partitionsMap.get(bestI));
				mergedIJ.addAll(partitionsMap.get(bestJ));
				presentSolution.add(mergedIJ);
			}
		}

        if (unReferenced.size() > 0) {
            presentSolution.add(unReferenced);
        }
		partitions = PartitioningUtils.getPartitioningMap(presentSolution);
        costCalculator.findPartitionsCost(partitions);

        bestSolutions = workload.getBestSolutions();

        /* We reduce the partition IDs by 1 and therefore the values in the best solutions as well. */
        TIntObjectHashMap<TIntHashSet> newPartitions = new TIntObjectHashMap<TIntHashSet>();
        TIntObjectHashMap<TIntHashSet> newBestSolutions = new TIntObjectHashMap<TIntHashSet>();
        
        for (int p : partitions.keys()) {
            newPartitions.put(p - 1, partitions.get(p));
        }
        
        for (int q : bestSolutions.keys()) {
            newBestSolutions.put(q, new TIntHashSet());
            for (int p : bestSolutions.get(q).toArray()) {
                newBestSolutions.get(q).add(p - 1);
            }
        }

        partitions = newPartitions;
        bestSolutions = newBestSolutions;
	}

	/**
	 * Method for determining the query extent of a fragment, that is the
	 * cardinality of the set of queries that reference all of the attributes in
	 * a fragment.
	 * 
	 * @param fragment
	 *            The input.
	 * @return The cardinality of the fragment's query extent.
	 */
	private int queryExtent(TIntSet fragment) {
		int size = 0;

		for (int q = 0; q < w.queryCount; q++) {
			boolean referencesAll = true;

			for (TIntIterator it = fragment.iterator(); it.hasNext(); ) {
				if (w.usageMatrix[q][it.next()] == 0) {
					referencesAll = false;
				}
			}

			if (referencesAll) {
				size++;
			}
		}

		return size;
	}

	/**
	 * Method for adding a fragment to a partitioning with removing any of the
	 * subsets of the fragment from the partitioning. Note that this method does
	 * not clone the input partitioning, therefore it returns the modified input
	 * instead of a cloned one.
	 * 
	 * @param partitioning
	 *            The partitioning to be extended.
	 * @param fragment
	 *            The partition to be added.
	 * @return The modified partitioning.
	 */
	private HashSet<TIntHashSet> addFragment(HashSet<TIntHashSet> partitioning, TIntHashSet fragment) {

		HashSet<TIntHashSet> toBeRemoved = new HashSet<TIntHashSet>();

		for (TIntHashSet F1 : partitioning) {
			boolean subset = true;
			for (TIntIterator it = F1.iterator(); it.hasNext(); ) {
				if (!fragment.contains(it.next())) {
					subset = false;
					break;
				}
			}

			if (subset) {
				toBeRemoved.add(F1);
			}
		}

		partitioning.removeAll(toBeRemoved);
		partitioning.add(fragment);

		return partitioning;
	}

	/**
	 * Method for calculating the row size of the partitioned table considering
	 * overlaps, too.
	 * 
	 * @param partitions
	 *            The set of possibly overlapping partitions.
	 * @return The calculated row size.
	 */
	private int getOverlappingPartitionsSize(HashSet<TIntHashSet> partitions) {
		int size = 0;

		for (TIntHashSet partition : partitions) {
			for (TIntIterator it = partition.iterator(); it.hasNext(); ) {
				size += w.attributeSizes[it.next()];
			}
		}

		return size;
	}

    /**
     * Method for calculating the row size of the partitioned table considering
     * overlaps, too.
     *
     * @param partitions
     *            The set of possibly overlapping partitions.
     * @return The calculated row size.
     */
    public int getOverlappingPartitionsSize(TIntObjectHashMap<TIntHashSet> partitions) {
        int size = 0;

        for (TIntHashSet partition : partitions.valueCollection()) {
            for (TIntIterator it = partition.iterator(); it.hasNext(); ) {
                size += w.attributeSizes[it.next()];
            }
        }

        return size;
    }

	public double getReplicationFactor() {
		return replicationFactor;
	}

	public void setReplicationFactor(double replicationFactor) {
		this.replicationFactor = replicationFactor;
	}

	public int getQueryExtentThreshold() {
		return queryExtentThreshold;
	}

	public void setQueryExtentThreshold(int queryExtentThreshold) {
		this.queryExtentThreshold = queryExtentThreshold;
	}
}
