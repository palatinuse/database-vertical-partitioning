package core.costmodels;

import db.schema.entity.Workload;

/**
 * HDD cost model that also considers selectivity.
 *
 * @author Endre Palatinus
 */
public class HDDSelectivityCostModel extends HDDCostModel {

    public HDDSelectivityCostModel(Workload.SimplifiedWorkload w, long bufferSize, long blockSize, double seekTime, double readDiskBW, double writeDiskBW) {
        super(w, bufferSize, blockSize, seekTime, readDiskBW, writeDiskBW);
        this.type = CMType.HDDSelectivity;
    }

    @Override
    public double getCostForPartition(int partitionRowSize, int referencedPartitionsRowSize) {
        return getCostForPartition(partitionRowSize, referencedPartitionsRowSize, 1.0, 1.0);
    }

    /**
     * Cost calculation considering selectivity as well.
     * @param shareFactor The factor for optimizing buffer space consumption considering selectivities.
     * @param selectivity The selectivity of the current query.
     */
    public double getCostForPartition(int partitionRowSize, int referencedPartitionsRowSize,
                                      double shareFactor, double selectivity) {

        /*
		 * We assume that all referenced partitions share the same buffer. The following is
		 * the memory size occupied by the current partition in the buffer.
		 */
        double partitionBufferSize = Math.max(
                Math.floor((double) bufferSize * partitionRowSize / referencedPartitionsRowSize * shareFactor),
                blockSize); // we have to read at least one block from the disk
        /*
         * This is the number of blocks that fit into the buffer for the current partition.
         */
        double blocksReadPerBuffer = Math.floor(partitionBufferSize / blockSize);
        /*
         * This is the total number of blocks of the current partition.
         */
        double numberOfBlocks = Math.ceil((double) partitionRowSize * w.numRows / blockSize) * selectivity;

        /* Time spent on seeking and scanning. */
        double seekCost = seekTime * Math.ceil(numberOfBlocks / blocksReadPerBuffer);
        double scanCost = numberOfBlocks * blockSize / readDiskBW;

        return seekCost + scanCost;
    }

    @Override
    public double[] getCostsForPartition(int partitionRowSize, int referencedPartitionsRowSize) {
        return getCostsForPartition(partitionRowSize, referencedPartitionsRowSize, 1.0);
    }


    /**
     * Cost calculation considering selectivity as well.
     * @param ratioOfBlocksToRead The ratioOfBlocksToRead for the current query and partition.
     */
    public double[] getCostsForPartition(int partitionRowSize, int referencedPartitionsRowSize,
                                         double ratioOfBlocksToRead) {

        /*
		 * We assume that all referenced partitions share the same buffer. The following is
		 * the memory size occupied by the current partition in the buffer.
		 */
        double partitionBufferSize = Math.max(
                Math.floor((double) bufferSize * partitionRowSize / referencedPartitionsRowSize),
                blockSize); // we have to read at least one block from the disk
        /*
         * This is the number of blocks that fit into the buffer for the current partition.
         */
        double blocksReadPerBuffer = Math.floor(partitionBufferSize / blockSize);
        /*
         * This is the total number of blocks of the current partition.
         */
        double numberOfBlocks = Math.ceil((double) partitionRowSize * w.numRows / blockSize * ratioOfBlocksToRead);

        /* Time spent on seeking and scanning. */
        double seekCost = seekTime * Math.ceil(numberOfBlocks / blocksReadPerBuffer)    // seek each time the buffer gets full
                * (1 + Math.ceil(blocksReadPerBuffer * (1.0 - ratioOfBlocksToRead)));   // seek after each skipped block
        double scanCost = numberOfBlocks * blockSize / readDiskBW;

        double[] costs = new double[2];
        costs[SEEK] = seekCost;
        costs[SCAN] = scanCost;

        return costs;
    }
}
