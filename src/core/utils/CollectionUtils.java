package core.utils;

import gnu.trove.iterator.TIntIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Class containing utility methods for collections.
 * 
 * @author Endre Palatinus
 * 
 */
public class CollectionUtils {

	/**
	 * Creates a deep copy of a HashMap<Integer, HashSet<Integer>>.
	 */
	public static Map<Integer, HashSet<Integer>> deepClone(Map<Integer, HashSet<Integer>> map) {
		HashMap<Integer, HashSet<Integer>> clone = new HashMap<Integer, HashSet<Integer>>();

		for (Integer key : map.keySet()) {
			clone.put(key, new HashSet<Integer>());

			clone.get(key).addAll(map.get(key));
		}

		return clone;
	}

	/**
	 * Creates a deep copy of a HashSet<HashSet<Integer>>.
	 *
	public static HashSet<HashSet<Integer>> deepClone(HashSet<HashSet<Integer>> set) {
		HashSet<HashSet<Integer>> clone = new HashSet<HashSet<Integer>>();

		for (HashSet<Integer> elements : set) {
			HashSet<Integer> newElements = new HashSet<Integer>();
			newElements.addAll(elements);

			clone.add(newElements);
		}

		return clone;
	} */

	/**
	 * Creates a deep copy of a TIntObjectHashMap<TIntHashSet>.
	 */
	public static TIntObjectHashMap<TIntHashSet> deepClone(TIntObjectHashMap<TIntHashSet> map) {
		
		TIntObjectHashMap<TIntHashSet> clone = new TIntObjectHashMap<TIntHashSet>();

		for (TIntIterator keyit = map.keySet().iterator(); keyit.hasNext(); ) {
			int key = keyit.next();
			
			clone.put(key, new TIntHashSet(map.get(key).capacity()));
			clone.get(key).addAll(map.get(key));
		}

		return clone;
	}
	
	/**
	 * Creates a deep copy of a HashSet<TIntHashSet>.
	 */
	public static HashSet<TIntHashSet> deepClone(HashSet<TIntHashSet> set) {
		
		HashSet<TIntHashSet> clone = new HashSet<TIntHashSet>();

		for (TIntHashSet elements : set) {
			TIntHashSet newElements = new TIntHashSet(elements.capacity()); 
			newElements.addAll(elements);

			clone.add(newElements);
		}

		return clone;
	}
}
