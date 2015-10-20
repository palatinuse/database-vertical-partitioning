package core.costmodels;

import db.schema.entity.Workload;

public abstract class CostModel implements Cloneable {

    /** Constants used to index the costs array returned by the getCostsForPartition method. */
    public static final int SEEK = 0;
    public static final int SCAN = 1;

    public static enum CMType {HDDSelectivity, HDD, MM}

    public CMType type;

    protected Workload.SimplifiedWorkload w;

	public static CostModel getCostModel(CMType model, Workload.SimplifiedWorkload w) {

		switch (model) {
		case HDD:
			return new HDDCostModel(w);
		default:
			throw new UnsupportedOperationException("Cost Model Not Known!");
		}
	}

	public CostModel(Workload.SimplifiedWorkload w) {
		this.w = w;
	}

    public abstract CostModel clone();

	/**
	 * Cost of a some query accessing a given partition. This function should
	 * not be called on partitions not referenced by the query.
	 *
	 * @param partitionRowSize
	 *            The row size of the partition.
     * @param referencedPartitionsRowSize
     *            The sum of the row sizes of the partitions referenced by the query.
	 * @return The calculated cost.
	 */
	public abstract double getCostForPartition(int partitionRowSize, int referencedPartitionsRowSize);

    /**
     * Same as getCostForPartition, but return seek- and scan costs separately.
     * @return An array with 2 doubles, the first being the seek- and the second the scan cost.
     */
    public abstract double[] getCostsForPartition(int partitionRowSize, int referencedPartitionsRowSize);

    public Workload.SimplifiedWorkload getW() {
        return w;
    }
    
    public void setW(Workload.SimplifiedWorkload w) {
        this.w = w;
    }
}
