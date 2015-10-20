package core.metrics;

import db.schema.entity.Workload;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A profiler class for (possibly) overlapping partitions.
 */
public final class PartitionsProfiler {

    public long candidateSetSize;
    public long numberOfIterations;

    private Workload.SimplifiedWorkload w;

    public PartitionsProfiler(Workload.SimplifiedWorkload w) {
        this.w = w;
    }

    /**
     * Get the total size of the table.
     * @return The size in bytes.
     */
    public long getTableSize() {
        return (long)w.rowSize * w.numRows;
    }

    /**
     * Method for calculating the total amount of bytes read due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of bytes read.
     */
    public long totalDataRead(TIntObjectHashMap<TIntHashSet> partitioningMap,
                                               TIntObjectHashMap<TIntHashSet> solutions) {

        long redundant = redundantBytesReadPerRow(partitioningMap, solutions);
        long useful = PartitioningProfiler.calculateReferencedDataSizePerRow(w);

        return (useful + redundant) * w.numRows;
    }

    /**
     * Method for calculating the total amount of redundant bytes read due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of extra bytes read.
     */
    public double fractionOfRedundantBytesRead(TIntObjectHashMap<TIntHashSet> partitioningMap,
                                               TIntObjectHashMap<TIntHashSet> solutions) {

        double redundant = redundantBytesReadPerRow(partitioningMap, solutions);
        int useful = PartitioningProfiler.calculateReferencedDataSizePerRow(w);

        return redundant / (useful + redundant);
    }

    /**
     * Method for calculating the total amount of redundant bytes read due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of extra bytes read.
     */
    public long redundantBytesReadPerTable(TIntObjectHashMap<TIntHashSet> partitioningMap,
                                          TIntObjectHashMap<TIntHashSet> solutions) {
        return (long)redundantBytesReadPerRow(partitioningMap, solutions) * w.numRows;
    }

    /**
     * Method for calculating the amount of redundant bytes read due to the
     * vertical (possibly overlapping) partitioning.
     *
     * @param partitioningMap
     *            The map from partition id to the set of attributes in the
     *            partition.
     * @param solutions
     *            The partition-selection plan for each query.
     * @return The amount of extra bytes read.
     */
    public int redundantBytesReadPerRow(TIntObjectHashMap<TIntHashSet> partitioningMap,
                                        TIntObjectHashMap<TIntHashSet> solutions) {

        int redundantBytesRead = 0;

        for (int q : solutions.keys()) {

            for (TIntIterator pit = solutions.get(q).iterator(); pit.hasNext();) {

                for (TIntIterator ait = partitioningMap.get(pit.next()).iterator(); ait.hasNext();) {
                    int a = ait.next();

                    if (w.usageMatrix[q][a] == 0) {
                        redundantBytesRead += w.attributeSizes[a];
                    }
                }
            }
        }

        return redundantBytesRead;
    }

    /**
     * Method for calculating the amount of joins needed due to the vertical
     * partitioning when executing all queries of the workload.
     *
     * @return The number of extra joins needed.
     */
    public long attributeJoinsPerTable(TIntObjectHashMap<TIntHashSet> solutions) {
        return (long)attributeJoinsPerRow(solutions) * w.numRows;
    }

    /**
     * Method for calculating the average number of joins needed due to the vertical
     * partitioning when executing all queries of the workload.
     *
     * @return The number of extra joins needed.
     */
    public double averageAttributeJoins(TIntObjectHashMap<TIntHashSet> solutions) {
        return (double)attributeJoinsPerRow(solutions) / w.queryCount;
    }

    /**
     * Method for calculating the amount of joins needed per row due to the vertical
     * (possibly overlapping) partitioning.
     *
     * @param solutions
     *            The partition-selection plan for each query.
     * @return The number of extra joins needed.
     */
    public int attributeJoinsPerRow(TIntObjectHashMap<TIntHashSet> solutions) {

        int redundantJoins = 0;

        for (TIntIterator qit = solutions.keySet().iterator(); qit.hasNext();) {

            redundantJoins += solutions.get(qit.next()).size() - 1;
        }

        return redundantJoins;
    }
}
