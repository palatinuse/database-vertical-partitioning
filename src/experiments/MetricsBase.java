package experiments;

import core.algo.vertical.AbstractAlgorithm;
import db.schema.BenchmarkTables;
import db.schema.entity.Table;

import java.util.TreeMap;

/**
 * Abstract base class for showing metrics for experiments.
 *
 * @author Endre Palatinus
 */
public abstract class MetricsBase {

    public static final int[] allQueries = new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21};

    protected TreeMap<Double, AbstractAlgorithm.AlgorithmConfig> configs;
    protected BenchmarkTables.BenchmarkConfig benchmarkConf;
    protected double scaleFactor;
    protected String paramName;
    protected Double[] paramValues;

    public MetricsBase(TreeMap<Double, AbstractAlgorithm.AlgorithmConfig> configs, String paramName) {
        this.configs = configs;
        this.paramName = paramName;
    }

    /**
     * Method for printing the metrics for each algorithms in tabular form. The user can specify more BenchmarkConfigs,
     * so as to investigate the effects of changing one parameter of the environment.
     * @param tableName The name of the benchmark.
     * @param configs The map from the parameter value being changed and the config set up with the given param value.
     */
    protected abstract void printMetrics(String tableName, TreeMap<Double, AbstractAlgorithm.AlgorithmConfig> configs);

    /**
     * Method for printing the partitionings produced for each algorithms in tabular form.
     * @param tableName The name of the benchmark.
     */
    protected abstract void printPartitions(String tableName);

    /**
     * Method for running the experiment for all TPC-H tables.
     */
    public void runTPC_H_Tables() {
        runTPC_H_Customer();
        runTPC_H_LineItem();
        runTPC_H_Orders();
        runTPC_H_Supplier();
        runTPC_H_Part();
        runTPC_H_PartSupp();
        runTPC_H_Nation();
        runTPC_H_Region();
    }

    /**
     * Method for running the experiment for TPC-H Customer.
     */
    public void runTPC_H_Customer() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchCustomer(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H LineItem.
     */
    public void runTPC_H_LineItem() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchLineitem(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        //RUN_OPTIMAL = false;
        printMetrics(table.name, configs);
        printPartitions(table.name);
        //RUN_OPTIMAL = true;
    }

    /**
     * Method for running the experiment for TPC-H Part.
     */
    public void runTPC_H_Part() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchPart(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H Supplier.
     */
    public void runTPC_H_Supplier() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchSupplier(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H PartSupp.
     */
    public void runTPC_H_PartSupp() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchPartSupp(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H Orders.
     */
    public void runTPC_H_Orders() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchOrders(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H Nation.
     */
    public void runTPC_H_Nation() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchNation(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for TPC-H Region.
     */
    public void runTPC_H_Region() {
        Table table = BenchmarkTables.partialTable(BenchmarkTables.tpchRegion(benchmarkConf), null, allQueries);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**********************************/
    /************** SSB ***************/

    /**
     * Method for running the experiment for all SSB tables.
     */
    public void runSSB_Tables() {
        runSSB_Customer();
        runSSB_LineOrder();
        runSSB_Supplier();
        runSSB_Part();
        runSSB_Date();
    }

    /**
     * Method for running the experiment for SSB Customer.
     */
    public void runSSB_Customer() {
        Table table = BenchmarkTables.ssbCustomer(benchmarkConf);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for SSB LineItem.
     */
    public void runSSB_LineOrder() {
        Table table = BenchmarkTables.ssbLineOrder(benchmarkConf);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for SSB Part.
     */
    public void runSSB_Part() {
        Table table = BenchmarkTables.ssbPart(benchmarkConf);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for SSB Supplier.
     */
    public void runSSB_Supplier() {
        Table table = BenchmarkTables.ssbSupplier(benchmarkConf);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }

    /**
     * Method for running the experiment for SSB Date.
     */
    public void runSSB_Date() {
        Table table = BenchmarkTables.ssbDate(benchmarkConf);

        for (Double paramValue : paramValues) {
            configs.get(paramValue).setTable(table);
        }

        printMetrics(table.name, configs);
        printPartitions(table.name);
    }
}