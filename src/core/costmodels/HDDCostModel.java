package core.costmodels;

import db.schema.entity.Workload;

/**
 * Hard disk I/O cost model for reading partitions by queries.
 *
 * @author Endre Palatinus
 */
public class HDDCostModel extends CostModel {

    /**
     * The block size of the DBMS.
     */
    public static final long DEFAULT_BLOCK_SIZE = 8 * 1024;
    /**
     * The buffer size of the DBMS.
     */
    public static final long DEFAULT_BUFFER_SIZE = 1024 * DEFAULT_BLOCK_SIZE;

    /**
     * Seek time.
     */
    protected double seekTime = 0.008;
    /**
     * The read bandwidth of the disk.
     */
    protected double readDiskBW = 92 * 1024 * 1024;

    /**
     * The write bandwidth of the disk.
     */
    protected double writeDiskBW = 70 * 1024 * 1024;

    /**
     * The block size of the DBMS.
     */
    protected long blockSize;
    /**
     * The I/O buffer size of the DBMS.
     */
    protected long bufferSize;

    /**
     * Constructor with default buffer- and block size.
     *
     * @param w The workload for which we shall calculate query-partition-wise
     *          I/O cost.
     */
    public HDDCostModel(Workload.SimplifiedWorkload w) {
        super(w);
        type = CMType.HDD;
        bufferSize = DEFAULT_BUFFER_SIZE;
        blockSize = DEFAULT_BLOCK_SIZE;
    }

    /**
     * Constructor with non-default buffer- and block sizes.
     *
     * @param w          The workload for which we shall calculate query-partition-wise
     *                   I/O cost.
     * @param bufferSize The size of the buffer.
     * @param blockSize  The size of the blocks read.
     */
    public HDDCostModel(Workload.SimplifiedWorkload w, long bufferSize, long blockSize) {
        super(w);
        type = CMType.HDD;
        this.bufferSize = bufferSize;
        this.blockSize = blockSize;
    }

    /**
     * Constructor with non-default buffer- and block sizes.
     *
     * @param w          The workload for which we shall calculate query-partition-wise
     *                   I/O cost.
     * @param bufferSize The size of the buffer.
     * @param blockSize  The size of the blocks read.
     * @param seekTime   The seek time.
     * @param readDiskBW    The bandwidth of the disk.
     */
    public HDDCostModel(Workload.SimplifiedWorkload w, long bufferSize, long blockSize, double seekTime,
                        double readDiskBW, double writeDiskBW) {
        super(w);
        type = CMType.HDD;
        this.bufferSize = bufferSize;
        this.blockSize = blockSize;
        this.seekTime = seekTime;
        this.readDiskBW = readDiskBW;
        this.writeDiskBW = writeDiskBW;
    }

    public HDDCostModel clone() {
        return new HDDCostModel(w, bufferSize, blockSize, seekTime, readDiskBW, writeDiskBW);
    }

    @Override
    public double getCostForPartition(int partitionRowSize, int referencedPartitionsRowSize) {

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
        double numberOfBlocks = Math.ceil((double) partitionRowSize * w.numRows / blockSize);

        /* Time spent on seeking and scanning. */
        double seekCost = seekTime * Math.ceil(numberOfBlocks / blocksReadPerBuffer);
        double scanCost = numberOfBlocks * blockSize / readDiskBW;

        return seekCost + scanCost;
    }

    @Override
    public double[] getCostsForPartition(int partitionRowSize, int referencedPartitionsRowSize) {

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
        double numberOfBlocks = Math.ceil((double) partitionRowSize * w.numRows / blockSize);

        /* Time spent on seeking and scanning. */
        double seekCost = seekTime * Math.ceil(numberOfBlocks / blocksReadPerBuffer);
        double scanCost = numberOfBlocks * blockSize / readDiskBW;

        double[] costs = new double[2];
        costs[SEEK] = seekCost;
        costs[SCAN] = scanCost;

        return costs;
    }

    public double getReadDiskBW() {
        return readDiskBW;
    }

    public void setReadDiskBW(double readDiskBW) {
        this.readDiskBW = readDiskBW;
    }

    public double getWriteDiskBW() {
        return writeDiskBW;
    }

    public void setWriteDiskBW(double writeDiskBW) {
        this.writeDiskBW = writeDiskBW;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public double getSeekTime() {
        return seekTime;
    }

    public long getBufferSize() {
        return bufferSize;
    }
}
