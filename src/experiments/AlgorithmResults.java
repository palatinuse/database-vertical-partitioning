package experiments;

import core.algo.vertical.AbstractAlgorithm;
import core.algo.vertical.AbstractPartitionsAlgorithm;
import core.algo.vertical.AutoPart;
import core.algo.vertical.DreamPartitioner;
import core.utils.ArrayUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.TreeSet;

/**
 * A collection of the results of partitioning algorithms and some methods for serializing the results.
 *
 * @author Endre Palatinus
 */
public class AlgorithmResults {

    /** The resulting partitions computed by the algorithms per table and algorithm. */
    public HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> partitions;
    /** The resulting best solutions computed by the overlapping algorithms per table and algorithm. */
    public HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>> bestSolutions;
    /** The runtimes of the algorithms per table and algorithm. */
    public HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>> runTimes;

    /**
     * Default constructor that initializes the collections.
     */
    public AlgorithmResults() {
        partitions = new HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>>();
        bestSolutions = new HashMap<String, HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>>();
        runTimes = new HashMap<String, HashMap<AbstractAlgorithm.Algo, Double>>();
    }

    /**
     * Convenience method for creating storage for the results obtained for a given table.
     * @param tableName The name of the table.
     */
    public void addResultsForTable(String tableName) {
        partitions.put(tableName, new HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>());
        bestSolutions.put(tableName, new HashMap<AbstractAlgorithm.Algo, TIntObjectHashMap<TIntHashSet>>());
        runTimes.put(tableName, new HashMap<AbstractAlgorithm.Algo, Double>());
    }

    /**
     * Store algorithm results for a given table.
     * @param tableName Table name.
     * @param algorithm Algorithm class containing the results.
     * @param runTime The average runtime of the algorithm.
     */
    public void storeResults(String tableName, AbstractAlgorithm algorithm, double runTime) {
        partitions.get(tableName).put(algorithm.type, algorithm.getPartitions());
        if (algorithm instanceof AutoPart || algorithm instanceof DreamPartitioner) {
            bestSolutions.get(tableName).put(algorithm.type, ((AbstractPartitionsAlgorithm)algorithm).getBestSolutions());
        }
        runTimes.get(tableName).put(algorithm.type, runTime);
    }

    /**
     * Store algorithm results for a given table.
     * @param tableName Table name.
     * @param algorithm Algorithm class containing the results.
     */
    @Deprecated
    public void storeDummyResults(String tableName, AbstractAlgorithm algorithm) { // _TODO remove this method?
        partitions.get(tableName).put(algorithm.type, null);
        if (algorithm instanceof AutoPart || algorithm instanceof DreamPartitioner) {
            bestSolutions.get(tableName).put(algorithm.type, null);
        }
        runTimes.get(tableName).put(algorithm.type, Double.NaN);
    }

    /**
     * Method for exporting the resulting partitions, best solutions (in case of overlaps)
     * and runtimes for each table-algorithm pair.
     * @param results The container of the necessary data.
     * @return The string representation of the results.
     */
    public static String exportResults(AlgorithmResults results) {
        StringBuilder sb = new StringBuilder();

        for (String tableName : results.partitions.keySet()) {

            sb.append(tableName).append("\n");

            TreeSet<AbstractAlgorithm.Algo> algos = new TreeSet<AbstractAlgorithm.Algo>(
                    results.partitions.get(tableName).keySet() );

            for (AbstractAlgorithm.Algo algo : algos) {

                sb.append(algo.ordinal()).append(" #" + algo.name() + "\n");

                /* Output runtime. */
                sb.append(results.runTimes.get(tableName).get(algo)).append(" #seconds computation time\n");

                /* Output partitions. */
                int partitionCount = results.partitions.get(tableName).get(algo).size();
                sb.append(partitionCount).append(" #partition count -- partitions:\n");
                int[] partitions = Arrays.copyOf(results.partitions.get(tableName).get(algo).keys(), partitionCount);
                Arrays.sort(partitions);

                for (int p : partitions) {
                    int[] partition = results.partitions.get(tableName).get(algo).get(p).toArray();
                    sb.append(ArrayUtils.arrayToString(partition, " ")).append('\n');
                }

                /* Output best solutions. */
                if (algo.equals(AbstractAlgorithm.Algo.AUTOPART) || algo.equals(AbstractAlgorithm.Algo.DREAM) ) {

                    int queryCount = results.bestSolutions.get(tableName).get(algo).size();
                    sb.append(queryCount).append(" #query count -- best solutions:\n");
                    int[] queries = Arrays.copyOf(results.bestSolutions.get(tableName).get(algo).keys(), queryCount);
                    Arrays.sort(queries);

                    for (int q : queries) {
                        int[] partitionSelectionPlan = results.bestSolutions.get(tableName).get(algo).get(q).toArray();
                        sb.append(ArrayUtils.arrayToString(partitionSelectionPlan, " ")).append('\n');
                    }
                }
            }

            sb.append('\n'); // print new line after each table
        }

        return sb.toString();
    }

    /**
     * Method for getting the first integer from a line possibly containing comments at the end as well.
     * @param s The line containing an integer at the start and maybe some additional text after that.
     * @return The first integer on the line.
     */
    public static int firstIntToken(String s) {
        return Integer.parseInt(s.substring(0, s.indexOf(" ")));
    }

    /**
     * Method for getting the first decimal value from a line possibly containing comments at the end as well.
     * @param s The line containing an decimal value at the start and maybe some additional text after that.
     * @return The first decimal value on the line.
     */
    public static double firstDoubleToken(String s) {
        return Double.parseDouble(s.substring(0, s.indexOf(" ")));
    }

    /**
     * Deserialize partitioning results from the string representation for a single table.
     * @param reader The reader for the string representation.
     * @return The container of to the resulting partitionings, best solutions and runtimes.
     */
    public static AlgorithmResults readResults(BufferedReader reader) {

        AlgorithmResults results = new AlgorithmResults();

        try {

            String tableName = reader.readLine();

            while (tableName != null) {
                results.addResultsForTable(tableName);

                String algoString;
                while ((algoString = reader.readLine()) != null && !algoString.equals("")) {

                    int algoOrdinal = firstIntToken(algoString);
                    AbstractAlgorithm.Algo algo = AbstractAlgorithm.Algo.values()[algoOrdinal];

                    /* Read runtime. */
                    double runtime = firstDoubleToken(reader.readLine());
                    results.runTimes.get(tableName).put(algo, runtime);

                    /* Read partitions. */
                    TIntObjectHashMap<TIntHashSet> partitions = new TIntObjectHashMap<TIntHashSet>();

                    int partitionCount = firstIntToken(reader.readLine());
                    for (int p = 0; p < partitionCount; p++) {
                        TIntHashSet partition = new TIntHashSet();
                        StringTokenizer st = new StringTokenizer(reader.readLine(), " ");
                        while (st.hasMoreTokens()) {
                            partition.add(Integer.parseInt(st.nextToken()));
                        }
                        partitions.put(p, partition);
                    }

                    results.partitions.get(tableName).put(algo, partitions);

                    if (algo.equals(AbstractAlgorithm.Algo.AUTOPART) || algo.equals(AbstractAlgorithm.Algo.DREAM) ) {
                        /* Read best solutions. */
                        TIntObjectHashMap<TIntHashSet> bestSolutions = new TIntObjectHashMap<TIntHashSet>();

                        int queryCount = firstIntToken(reader.readLine());
                        for (int q = 0; q < queryCount; q++) {
                            TIntHashSet partitionSelectionPlan = new TIntHashSet();
                            StringTokenizer st = new StringTokenizer(reader.readLine(), " ");
                            while (st.hasMoreTokens()) {
                                partitionSelectionPlan.add(Integer.parseInt(st.nextToken()));
                            }
                            bestSolutions.put(q, partitionSelectionPlan);
                        }

                        results.bestSolutions.get(tableName).put(algo, bestSolutions);
                    }
                }

                tableName = null;
                if (algoString != null) {
                    tableName = reader.readLine();
                }
            }
        } catch (IOException ex) {
            System.out.println("Error in file IO!");
        } catch (Exception ex) {
            System.out.println(ex.getStackTrace());
        }

        return results;
    }
}
