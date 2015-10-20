package db.schema.utils;

import core.utils.ArrayUtils;
import db.schema.entity.Query;
import db.schema.entity.Workload;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.ArrayList;
import java.util.List;

public class WorkloadUtils {

	public static String[] getQueryNames(Workload w){
		String[] names = new String[w.queries.size()];
		for(int i=0;i<names.length;i++)
			names[i] = w.queries.get(i).getName();
		return names;
	}
	
	public static int[] getQueryWeights(Workload w){
		int[] weights = new int[w.queries.size()];
		for(int i=0;i<weights.length;i++)
			weights[i] = w.queries.get(i).getWeight();
		return weights;
	}

    /**
     * Method for creating a subset of queries with an attribute mapping.
     * @param queries The list of queries to filter.
     * @param ids The list of query ids to include in the subset.
     * @param attributeIdx The mapping between the attribute IDs of the original attribute set and the reduced attribute
     *                     set containing only the attributes that are referenced by the query subset.
     * @return The filtered query subset on the reduced attribute set.
     */
	public static List<Query> getQuerySubset(List<Query> queries, int[] ids, TIntIntHashMap attributeIdx){
		List<Query> subset = new ArrayList<Query>();
		for(int id: ids) {
            if (id < queries.size()) {
                subset.add(QueryUtils.getPartialQuery(queries.get(id), attributeIdx)); 
            }
        }
		return subset;
	}

    /**
     * Method for creating a subset of queries with an attribute mapping.
     * @param queries The list of queries to filter.
     * @param names The list of query names to include in the subset.
     * @param attributeIdx The mapping between the attribute IDs of the original attribute set and the reduced attribute
     *                     set containing only the attributes that are referenced by the query subset.
     * @return The filtered query subset on the reduced attribute set.
     */
    public static List<Query> getQuerySubset(List<Query> queries, String[] names, TIntIntHashMap attributeIdx){
        List<Query> subset = new ArrayList<Query>();
        
        for(String name: names) {    
            for (Query q : queries) {
                if (name.equals(q.getName())) {
                    subset.add(QueryUtils.getPartialQuery(q, attributeIdx));
                }
            }
        }

        return subset;
    }

    /**
     * Method for creating a list of attributes that are referenced by a given set of queries.
     * @param usageM The usage matrix for the whole workload.
     * @param queries The list of queries to check to referenced attributes for.
     * @return The array referenced attributes in ascending order.
     */
	public static int[] getReferencedAttributes(int[][] usageM, int[] queries){

        TIntArrayList attrsList = new TIntArrayList();

        for(int i=0;i<usageM[0].length;i++){
			for(int q: queries){
				if(usageM[q][i] > 0){
                    attrsList.add(i);
					break;
				}
			}
		}
		return attrsList.toArray();
	}
	
	public static TIntHashSet getNonReferencedAttributes(int[][] usageM){
        TIntHashSet nonRefAttrs = new TIntHashSet();
		for(int i=0;i<usageM[0].length;i++)
			nonRefAttrs.add(i);
		
		for(int a: getReferencedAttributes(usageM, ArrayUtils.simpleArray(usageM.length, 0, 1)))
			nonRefAttrs.remove(a);
		
		return nonRefAttrs;
	}
	
	public static int[][] getSubMatrix(int[][] usageM, int[] queries, int[] attrs){
		int[][] newUsageM = new int[queries.length][attrs.length];
		
		for(int i=0;i<newUsageM.length;i++){
			for(int j=0;j<newUsageM[i].length;j++){
				newUsageM[i][j] = usageM[queries[i]][attrs[j]];
			}
		}
		
		return newUsageM;
	}
}
