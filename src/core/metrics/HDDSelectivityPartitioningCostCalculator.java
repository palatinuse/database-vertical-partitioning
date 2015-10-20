package core.metrics;

import core.algo.vertical.AbstractAlgorithm;
import core.costmodels.CostModel;
import core.costmodels.HDDSelectivityCostModel;
import gnu.trove.set.hash.TIntHashSet;

/**
 * An HDD partitioning cost calculator that considers selectivity as well.
 *
 * @author Endre Palatinus
 */
public class HDDSelectivityPartitioningCostCalculator extends HDDPartitioningCostCalculator {

    public HDDSelectivityPartitioningCostCalculator(AbstractAlgorithm.HDDAlgorithmConfig config) {
        super(config);
    }

    @Override
    /**
     * Calculate the sum of the I/O cost through each query imposed by all partitions, considering selectivity.
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
            /* Ratio of blocks to read from each partition. */
            double[] ratioOfBlocksToRead = new double[partitions.length];

            for (int p = 0; p < partitions.length; p++) {

                isReferenced[p] = false;

                for (int a : partitions[p]) {
                    if (w.usageMatrix[q][a] == 1) {
                        isReferenced[p] = true;
                        referencedPartitionsRowSize += partitionRowSize[p];

                        // calculate the ratio of blocks read for each referenced partitions

                        TIntHashSet partition = new TIntHashSet(partitions[p]);
                        // does this partition contain selectivity attributes
                        if (partition.removeAll(w.selectivityColumns[q])) {
                            // if yes, do a full scan
                            ratioOfBlocksToRead[p] =  1.0;
                        } else {
                            long rowsPerBlock = cm.getBlockSize() / partitionRowSize[p];
                            long jump = (long) (1 / w.selectivities[q]); // difference between ROWIDs of read rows

                            if (jump < rowsPerBlock) {
                                // we can't skip any blocks, so we do a full scan
                                ratioOfBlocksToRead[p] =  1.0;
                            } else {
                                long currentPosition = 0; // current ID of row to read
                                long readBlocks = 0; // count of blocks that have to be read

                                do {
                                    currentPosition += jump;
                                    readBlocks++;
                                } while (currentPosition % rowsPerBlock != 0);

                                ratioOfBlocksToRead[p] = (double)readBlocks / (currentPosition / rowsPerBlock);
                            }
                        }

                        break;
                    }
                }
            }

            for (int p = 0; p < partitions.length; p++) {
                if (isReferenced[p]) {

                    double[] costsForQueryFullScan = ((HDDSelectivityCostModel)cm).getCostsForPartition(
                            partitionRowSize[p], referencedPartitionsRowSize, 1.0);

                    // calculate the costs of reading the partition with skipping some blocks, if possible
                    if (ratioOfBlocksToRead[p] < 1.0) {

                        double[] costsForQuerySelective = ((HDDSelectivityCostModel)cm).getCostsForPartition(
                                partitionRowSize[p], referencedPartitionsRowSize, ratioOfBlocksToRead[p]);

                        // if selective reading is cheaper than full scan, we choose that one
                        if (costsForQueryFullScan[CostModel.SEEK] + costsForQueryFullScan[CostModel.SCAN] >
                                costsForQuerySelective[CostModel.SEEK] + costsForQuerySelective[CostModel.SCAN]) {

                            costs[i][CostModel.SEEK] += costsForQuerySelective[CostModel.SEEK];
                            costs[i][CostModel.SCAN] += costsForQuerySelective[CostModel.SCAN];
                            continue;
                        }
                    }

                    costs[i][CostModel.SEEK] += costsForQueryFullScan[CostModel.SEEK];
                    costs[i][CostModel.SCAN] += costsForQueryFullScan[CostModel.SCAN];
                }
            }
        }

        return costs;
    }
}
