package core.algo.vertical;

import core.costmodels.CostModel;
import core.costmodels.HDDCostModel;
import core.utils.PartitioningUtils;
import core.utils.TimeUtils.Timer;
import db.schema.entity.Table;
import db.schema.entity.Workload;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

public abstract class AbstractAlgorithm {

	public static enum Algo {ROW, AUTOPART, HILLCLIMB, HYRISE, INTGBASED, NAVATHE, O2P, OPTIMAL, TROJAN, LIFT, DREAM, COLUMN}
	public Algo type;

	protected Workload workload;
	protected Workload.SimplifiedWorkload w;

	protected Table t;

	protected Timer timer;

    protected AlgorithmConfig config;

	public AbstractAlgorithm(AlgorithmConfig config) {

		// Why would a partitioning algorithm need to know an existing
		// partitioning?
		//this.partitions = t.partitions;

        this.config = config;

        this.t = config.getTable();
		this.workload = t.workload;
		this.w = workload.getSimplifiedWorkload();

		timer = new Timer();
	}

	public void partition() {
		timer.start();
        if (w.queryCount == 0) {
            int[] partitioning = new int[w.attributeCount];
            if (this instanceof AbstractPartitioningAlgorithm) {
                ((AbstractPartitioningAlgorithm)this).partitioning = partitioning;
            } else {
                ((AbstractPartitionsAlgorithm)this).partitions = PartitioningUtils.getPartitioningMap(partitioning);
                ((AbstractPartitionsAlgorithm)this).bestSolutions = new TIntObjectHashMap<TIntHashSet>();
            }
        } else {
		    doPartition();
        }
		timer.stop();
		t.partitions = getPartitions();
	}

	public abstract void doPartition();
	
	/**
	 * The collection of elements for each partition.
	 * 
	 * Note that this is the most general representation of the partitions, and
	 * internally non-overlapping partitions can be represented as a
	 * partitioning.
	 */
	public abstract TIntObjectHashMap<TIntHashSet> getPartitions();

	/*
	 * Four measures to evaluate a vertical partitioning method
	 * 
	 * 1. Total time taken to compute the partitioning 2. Total redundant bytes
	 * read due to the partitioning 3. Total attribute joins performed in the
	 * entire table
	 */

	public double getTimeTaken() {
		return timer.getElapsedTime();
	}

    /**
     * The number of elements of the search space of the algorithm.
     */
	public abstract long getCandidateSetSize();

	public abstract long getNumberOfIterations();

	public abstract static class AlgorithmConfig implements Cloneable {
		protected Table table;
        public Workload.SimplifiedWorkload w;
        public CostModel.CMType type;

		public AlgorithmConfig(Table table) {
			this.table = table;
            this.w = table.workload.getSimplifiedWorkload();
		}

		public Table getTable() {
			return table;
		}

        public void setTable(Table table) {
            this.table = table;
            this.w = table.workload.getSimplifiedWorkload();
        }

        public void setW(Workload.SimplifiedWorkload w) {
            this.w = w;
        }

        @Override
        public abstract AlgorithmConfig clone();
    }

    public static class HDDAlgorithmConfig extends AlgorithmConfig {

        public HDDCostModel costModel;

        public HDDAlgorithmConfig(Table table, HDDCostModel costModel) {
            super(table);

            this.costModel = costModel;
            this.type = costModel.type;
        }

        public HDDAlgorithmConfig(Table table) {
            super(table);

            this.costModel = (HDDCostModel)
                    CostModel.getCostModel(CostModel.CMType.HDD, table.workload.getSimplifiedWorkload());
            this.type = CostModel.CMType.HDD;
        }

        @Override
        public void setTable(Table table) {
            super.setTable(table);
            costModel.setW(table.workload.getSimplifiedWorkload());
        }

        @Override
        public void setW(Workload.SimplifiedWorkload w) {
            costModel = (HDDCostModel)CostModel.getCostModel(CostModel.CMType.HDD, w);
        }

        @Override
        public AlgorithmConfig clone() {
            return new HDDAlgorithmConfig(table, costModel);
        }
    }

    public static class MMAlgorithmConfig extends AlgorithmConfig {

        public MMAlgorithmConfig(Table table) {
            super(table);

            this.type = CostModel.CMType.MM;
        }

        @Override
        public AlgorithmConfig clone() {
            return new MMAlgorithmConfig(table);
        }
    }
	
	public static AbstractAlgorithm getAlgo(Algo algo, AlgorithmConfig config){
		switch(algo){
		case AUTOPART:	return new AutoPart(config);
		case HILLCLIMB:	return new HillClimb(config);
		case HYRISE:	return new HYRISE(config);
		case NAVATHE:	return new NavatheAlgorithm(config);
		case O2P:		return new O2P(config);
		case OPTIMAL:	return new Optimal(config);
		case TROJAN:	return new TrojanLayout(config);
        case DREAM:     return new DreamPartitioner(config);
		default:		return null;
		}
	}
}
