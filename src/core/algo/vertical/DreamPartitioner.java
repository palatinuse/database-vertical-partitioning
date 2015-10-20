package core.algo.vertical;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

/**
 * Vertical partitioner algorithm that for each query creates a partition perfectly tailored for it.
 * This is used for having a lower bound on query I/O costs.
 *
 * @author Endre Palatinus
 */
public class DreamPartitioner extends AbstractPartitionsAlgorithm {

    public DreamPartitioner(AlgorithmConfig config) {
        super(config);
        type = Algo.DREAM;
    }

    @Override
    public void doPartition() {
        
        bestSolutions = new TIntObjectHashMap<TIntHashSet>(w.queryCount);
        
        // We create one partition per query.
        partitions = new TIntObjectHashMap<TIntHashSet>(w.queryCount);

        // For each query's partition we add exactly the attributes it references.
        for (int q = 0; q < w.queryCount; q++) {
            TIntHashSet partition = new TIntHashSet(w.attributeCount);
            for (int a = 0; a < w.attributeCount; a++) {
                if (w.usageMatrix[q][a] == 1) {
                    partition.add(a);
                }
            }
            partitions.put(partitions.size(), partition);
            // route the query to the currently created partition
            bestSolutions.put(q, new TIntHashSet(new int[]{q}));
        }

        workload.setBestSolutions(bestSolutions);
    }
}
