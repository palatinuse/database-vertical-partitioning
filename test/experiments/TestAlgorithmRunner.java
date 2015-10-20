package experiments;

import org.junit.Test;

/**
 * Created by endre on 20/10/15.
 */
public class TestAlgorithmRunner {

    /**
     * Run all VP algos for the workload provided by Jiannan Wang.
     */
    @Test
    public void runTPCH_all() {
        AlgorithmRunner algorithmRunner = new AlgorithmRunner();
        algorithmRunner.runTPC_H_All();
        System.out.println(AlgorithmResults.exportResults(algorithmRunner.results));
    }
}
