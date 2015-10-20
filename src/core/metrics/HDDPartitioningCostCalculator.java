package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import core.costmodels.HDDCostModel;
import core.utils.PartitioningUtils;

/**
 * Class for cost computations for algorithms producing a non-overlapping partitioning.
 *
 * @author Endre Palatinus
 */
public class HDDPartitioningCostCalculator extends PartitioningCostCalculator {

    protected HDDCostModel cm;

    public HDDPartitioningCostCalculator(AbstractAlgorithm.HDDAlgorithmConfig config) {
        super(config);

        this.cm = config.costModel;
    }

    @Override
    /**
     * Calculate the sum of the I/O seek cost through all queries imposed by all partitions.
     */
    public double getPartitioningSeekCost(int[] partitioning) {
        return getPartitionsCosts(PartitioningUtils.getPartitions(partitioning), allQueries)[CostModel.SEEK];
    }

    @Override
    /**
     * Calculate the sum of the I/O scan cost through all queries imposed by all partitions.
     */
    public double getPartitioningScanCost(int[] partitioning) {
        return getPartitionsCosts(PartitioningUtils.getPartitions(partitioning), allQueries)[CostModel.SCAN];
    }

    @Override
    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     * @return An array with 2 doubles, the first being the seek- and the second the scan cost.
     */
    public double[] getPartitionsCosts(int[][] partitions, int[] queries) {

        double[] totalCosts = new double[2];
        double[][] costs = getPerQueryPartitionsCosts(partitions, queries);

        for (int i=0; i<queries.length; i++) {
            totalCosts[HDDCostModel.SEEK] += costs[i][HDDCostModel.SEEK];
            totalCosts[HDDCostModel.SCAN] += costs[i][HDDCostModel.SCAN];
        }

        return totalCosts;
    }

    @Override
    /**
     * Calculate the sum of the I/O cost through all queries imposed by all partitions.
     */
    public double getPartitionsCost(int[][] partitions, int[] queries) {

        double totalCost = 0.0;
        double[][] costs = getPerQueryPartitionsCosts(partitions, queries);

        for (int i=0; i<queries.length; i++) {
            totalCost += costs[i][HDDCostModel.SEEK] + costs[i][HDDCostModel.SCAN];
        }

        return totalCost;
    }

    /**
     * Calculate the sum of the I/O cost through each query imposed by all partitions.
     * @return An array with 2 doubles per query, the first being the seek- and the second the scan cost.
     */
    public double[][] getPerQueryPartitionsCosts(int[][] partitions, int[] queries) {

        /* Seek- and scan costs per query. */
        double[][] costs = new double[queries.length][2];

        /* The row sizes of the partitions. */
        int[] partitionRowSize = new int[partitions.length];

        for (int p = 0; p < partitions.length; p++) {

            for (int a : partitions[p]) {
                partitionRowSize[p] += w.attributeSizes[a];
            }
        }

        for (int i = 0; i < queries.length; i++) {
            int q = queries[i];

            /* The sum of the row sizes of the partitions referenced by the query. */
            int referencedPartitionsRowSize = 0;
            /* Denotes if the partition is referenced by the query or not. */
            boolean[] isReferenced = new boolean[partitions.length];

            for (int p = 0; p < partitions.length; p++) {

                isReferenced[p] = false;

                for (int a : partitions[p]) {
                    if (w.usageMatrix[q][a] == 1) {
                        isReferenced[p] = true;
                        referencedPartitionsRowSize += partitionRowSize[p];
                        break;
                    }
                }
            }

            for (int p = 0; p < partitions.length; p++) {
                if (isReferenced[p]) {
                    double[] costsForQuery = cm.getCostsForPartition(partitionRowSize[p], referencedPartitionsRowSize);
                    costs[i][HDDCostModel.SEEK] += costsForQuery[CostModel.SEEK];
                    costs[i][HDDCostModel.SCAN] += costsForQuery[CostModel.SCAN];
                }
            }
        }

        return costs;
    }

    @Override
    /**
     * Method for calculating the cost of transforming the current layout to the newly calculated partitioning.
     * @param sourcePartitioning The current layout.
     * @param targetPartitioning The newly calculated layout.
     * @return The cost of reading the current layout and writing the target layout.
     */
    public double layoutCreationCost(int[] sourcePartitioning, int[] targetPartitioning) {
        double cost = 0.0;

        // read source partitioning
        int[][] sourcePartitions = PartitioningUtils.getPartitions(sourcePartitioning);

        /* The row sizes of the partitions. */
        int[] partitionRowSize = new int[sourcePartitions.length];
        /* The sum of the row sizes of the partitions. */
        int totalPartitionsRowSize = 0;

        for (int p = 0; p < sourcePartitions.length; p++) {

            for (int a : sourcePartitions[p]) {
                partitionRowSize[p] += w.attributeSizes[a];
            }
            totalPartitionsRowSize += partitionRowSize[p];
        }

        for (int p = 0; p < sourcePartitions.length; p++) {
                cost += cm.getCostForPartition(partitionRowSize[p], totalPartitionsRowSize);
        }


        // write target partitioning
        int[][] targetPartitions = PartitioningUtils.getPartitions(targetPartitioning);

        partitionRowSize = new int[targetPartitions.length];
        totalPartitionsRowSize = 0;

        for (int p = 0; p < targetPartitions.length; p++) {

            for (int a : targetPartitions[p]) {
                partitionRowSize[p] += w.attributeSizes[a];
            }
            totalPartitionsRowSize += partitionRowSize[p];
        }

        // HACK: only suitable for HDDCostModel
        HDDCostModel HDDcm = (HDDCostModel)cm;
        double oldBW_disk = HDDcm.getReadDiskBW();
        HDDcm.setReadDiskBW(HDDcm.getWriteDiskBW());

        for (int p = 0; p < targetPartitions.length; p++) {
            cost += cm.getCostForPartition(partitionRowSize[p], totalPartitionsRowSize);
        }

        HDDcm.setReadDiskBW(oldBW_disk);

        return cost;
    }

    @Override
    /**
     * Method for calculating the cost of creating the layout for the newly calculated partitioning
     * by filling it with data read from a file.
     * @param targetPartitioning The newly calculated layout.
     * @return The cost of reading the file and writing date into the target layout.
     */
    public double layoutCreationCost(int[] targetPartitioning) {
        double cost = 0.0;

        int[][] targetPartitions = PartitioningUtils.getPartitions(targetPartitioning);

        int[] partitionRowSize = new int[targetPartitions.length];
        int totalPartitionsRowSize = 0;

        for (int p = 0; p < targetPartitions.length; p++) {

            for (int a : targetPartitions[p]) {
                partitionRowSize[p] += w.attributeSizes[a];
            }
            totalPartitionsRowSize += partitionRowSize[p];
        }

        // reading the file
        HDDCostModel HDDcm = (HDDCostModel)cm;
        long bufferSize = 1000000 * totalPartitionsRowSize;  // set buffer size to be the size of 1M rows.
        HDDCostModel readLayoutCM = new HDDCostModel(HDDcm.getW(), bufferSize, HDDcm.getBlockSize(), HDDcm.getSeekTime(),
                HDDcm.getReadDiskBW(), HDDcm.getWriteDiskBW());
        cost += readLayoutCM.getCostForPartition(totalPartitionsRowSize, totalPartitionsRowSize);

        // writing the partitions
        HDDCostModel writeLayoutCM = new HDDCostModel(HDDcm.getW(), HDDcm.getBufferSize(), HDDcm.getBlockSize(),
                HDDcm.getSeekTime(), HDDcm.getReadDiskBW(), HDDcm.getWriteDiskBW());
        writeLayoutCM.setReadDiskBW(writeLayoutCM.getWriteDiskBW());
        for (int p = 0; p < targetPartitions.length; p++) {
            cost += writeLayoutCM.getCostForPartition(partitionRowSize[p], totalPartitionsRowSize);
        }

        return cost;
    }
}
