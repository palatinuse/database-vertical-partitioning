package core.models;

import db.schema.entity.Workload;

public class HDDCostModelAlekh extends CostModel {
	private double C_rand = 0.005;
	private int blockSize = 256 * 1024 * 1024;
	private double bufferSize = 20 * 512 * 1024;
	private int BW_disk = 100 * 1024 * 1024;

	public HDDCostModelAlekh(Workload.SimplifiedWorkload w) {
		super(w);
		type = CMType.HDD;
	}

	public double getCost(int q, int[] partition) {
		return C_random(q, partition, refRowSize[q]) + C_scan(partition);
	}

	/**
	 * This method calculates the random I/O cost per block associated with a
	 * query accessing a given partition.
	 * 
	 * @param q
	 *            The index for the query.
	 * @param partition
	 *            The attributes forming a partition.
	 * @param refRowSize
	 *            The row size of the result set of the query.
	 * @return The seek time associated with the query considering only a given
	 *         partition.
	 */
	private double C_random(int q, int[] partition, int refRowSize) {
		int partitionRowSize = 0;
		int partitionRefRowSize = 0;
		for (int attribute : partition) {
			partitionRowSize += w.attributeSizes[attribute];
			if (w.usageM[q][attribute] == 1)
				partitionRefRowSize += w.attributeSizes[attribute];
		}

		/*
		 * We assume that attributes referenced by a given query in each
		 * partition, together forming the result set, share the same buffer.
		 * 
		 * The following is the memory size occupied by the referenced
		 * attributes of this partition in a single buffer.
		 */
		double partitionBuffer = bufferSize * partitionRefRowSize / refRowSize;

		/*
		 * We assume that each partition occupies a part of every block
		 * proportional to its size, that is each block is proportionally
		 * divided among the partitions.
		 * 
		 * The following is the number of times a random scan has to be
		 * performed, due to the part of the buffer for this partition getting
		 * full, multiplied by the seek time.
		 */
		return C_rand * Math.ceil((double) blockSize * partitionRowSize / rowSize / partitionBuffer);
	}

	/**
	 * This method calculates the sequential read (scan) cost per block
	 * associated with a query accessing a given partition.
	 * 
	 * @param partition
	 * @return
	 */
	private double C_scan(int[] partition) {
		int partitionRowSize = 0;
		for (int attribute : partition)
			partitionRowSize += w.attributeSizes[attribute];

		return (double) blockSize * partitionRowSize / rowSize / BW_disk;
	}
}
