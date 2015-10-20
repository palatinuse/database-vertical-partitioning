package core.metrics;

import core.utils.PartitioningUtils;
import db.schema.entity.Workload;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A profiler class for non-overlapping partitioning.
 *
 * @author Endre Palatinus
 */
public final class PartitioningProfiler {

    public long candidateSetSize;
    public long numberOfIterations;

    private Workload.SimplifiedWorkload w;

    private int[] allQueries;

    public PartitioningProfiler(Workload.SimplifiedWorkload w) {
        this.w = w;

        allQueries = new int[w.queryCount];
        for (int i = 0; i < allQueries.length; i++) {
            allQueries[i] = i;
        }
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
    public long totalDataRead(int[] partitioning) {

        long redundant = redundantBytesReadPerRow(partitioning, allQueries);
        long useful = calculateReferencedDataSizePerRow(w);

        return (useful + redundant) * w.numRows;
    }

    /**
     * Method for calculating the total amount of redundant bytes read due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of extra bytes read.
     */
    public double fractionOfRedundantBytesRead(int[] partitioning) {

        double redundant = redundantBytesReadPerRow(partitioning, allQueries);
        int useful = calculateReferencedDataSizePerRow(w);

        return redundant / (useful + redundant);
    }

    /**
     * Method for calculating the total amount of redundant bytes read due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of extra bytes read.
     */
    public long redundantBytesReadPerTable(int[] partitioning) {
        return (long)redundantBytesReadPerRow(partitioning, allQueries) * w.numRows;
    }

    /**
     * Method for calculating the amount of redundant bytes read per row due to the
     * vertical partitioning when executing all queries of the workload.
     *
     * @return The amount of extra bytes read.
     */
    public int redundantBytesReadPerRow(int[] partitioning) {
        return redundantBytesReadPerRow(partitioning, allQueries);
    }

    /**
     * Method for calculating the amount of redundant bytes read per row due to the
     * vertical partitioning when executing only a given subset of the
     * workload's queries.
     *
     * @param queries The subset of query ID-s for which the calculation should
     *                be restricted.
     * @return The amount of extra bytes read.
     */
    public int redundantBytesReadPerRow(int[] partitioning, int[] queries) {

        int redundantBytesRead = 0;
        int[][] partitions = PartitioningUtils.getPartitions(partitioning);

        for (int q : queries) {

            TIntHashSet referencedPartitions = new TIntHashSet(w.attributeCount);

            for (int a = 0; a < w.attributeCount; a++) {
                if (w.usageMatrix[q][a] == 1) {
                    referencedPartitions.add(partitioning[a]);
                }
            }

            for (TIntIterator pit = referencedPartitions.iterator(); pit.hasNext(); ) {
                int p = pit.next();

                for (int a : partitions[p]) {

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
    public long attributeJoinsPerTable(int[] partitioning) {
        return (long)attributeJoinsPerRow(partitioning, allQueries) * w.numRows;
    }

    /**
     * Method for calculating the amount of joins needed due to the vertical
     * partitioning when executing all queries of the workload.
     *
     * @return The number of extra joins needed.
     */
    public int attributeJoinsPerRow(int[] partitioning) {
        return attributeJoinsPerRow(partitioning, allQueries);
    }

    /**
     * Method for calculating the average number of joins needed due to the vertical
     * partitioning when executing all queries of the workload.
     *
     * @return The number of extra joins needed.
     */
    public double averageAttributeJoins(int[] partitioning) {
        return (double)attributeJoinsPerRow(partitioning, allQueries) / w.queryCount;
    }

    /**
     * Method for calculating the amount of joins needed per row due to the vertical
     * partitioning when executing only a given subset of the workload's
     * queries.
     *
     * @param queries The subset of query ID-s for which the calculation should
     *                be restricted.
     * @return The number of extra joins needed.
     */
    public int attributeJoinsPerRow(int[] partitioning, int[] queries) {

        int redundantJoins = 0;

        for (int q : queries) {

            TIntHashSet referencedPartitions = new TIntHashSet(w.attributeCount);

            for (int a = 0; a < w.attributeCount; a++) {
                if (w.usageMatrix[q][a] == 1) {
                    referencedPartitions.add(partitioning[a]);
                }
            }

            redundantJoins += referencedPartitions.size() - 1;
        }

        return redundantJoins;
    }

    /**
     * Calculate the size of the data that needs to be accessed per row for all queries.
     * @return The amount of data in bytes.
     */
    public static int calculateReferencedDataSizePerRow(Workload.SimplifiedWorkload w) {
        int totalReferencedData = 0;

        for (int q = 0; q < w.queryCount; q++) {

            for (int a = 0; a < w.attributeCount; a++) {
                if (w.usageMatrix[q][a] == 1) {
                    totalReferencedData += w.attributeSizes[a];
                }
            }
        }

        return totalReferencedData;
    }
}
