package core.utils;

import db.schema.entity.Table;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;

import java.util.*;

public class PartitioningUtils {

    /**
     * Method for creating a partitions representation from a non-overlapping
     * partitioning. The order of the attributes in each partition are sorted in ascending order.
     */
	public static int[][] getPartitions(int[] partitioning) {

		TIntObjectHashMap<TIntHashSet> partitions = new TIntObjectHashMap<TIntHashSet>();
		
		/* The biggest partition ID. */
		int maxP = 0;

		for (int i = 0; i < partitioning.length; i++) {

			if (!partitions.containsKey(partitioning[i])) {
				partitions.put(partitioning[i], new TIntHashSet(partitioning.length));
			}

			partitions.get(partitioning[i]).add(i);
			
			if (partitioning[i] > maxP) {
				maxP = partitioning[i];
			}
		}

		/* In order to remain consistent with the partitioning, we should create as many partitions as the maximal partition ID. */
		int[][] partitionsArray = new int[maxP + 1][0];

		for (TIntIterator pit = partitions.keySet().iterator(); pit.hasNext(); ) {
			int p = pit.next();

            TIntArrayList sortedPartition = new TIntArrayList(partitions.get(p).toArray());
            sortedPartition.sort();
			partitionsArray[p] = sortedPartition.toArray();
		}

		return partitionsArray;
	}

    /**
     * Method for converting a partitions-based(2D array) representation of a partitioning to a 1D-array representation.
     * @param partitions The input in partitions format.
     * @return The result in 1D-array format.
     */
	public static int[] getPartitioning(int[][] partitions) {
		int numAttributes = 0;
		for (int[] partition : partitions) {
			numAttributes += partition.length;
        }

        final int NOT_SET = -1;
		int[] partitioning = new int[numAttributes];
        for (int i = 0; i < numAttributes; i++) {
            partitioning[i] = NOT_SET;
        }

		for (int i = 0; i < partitions.length; i++) {
			for (int j = 0; j < partitions[i].length; j++) {
                if (partitioning[partitions[i][j]] != NOT_SET) {
                    throw new IllegalArgumentException("ERROR: The specified partitioning is overlapping!");
                }
				partitioning[partitions[i][j]] = i;
			}
		}

		return partitioning;
	}

	public static int[] getPartitioning(List<int[][]> columnGroupSets) {
		List<int[]> columnGroups = new ArrayList<int[]>();
		int size = 0;
		for (int[][] columnGroupSet : columnGroupSets) {
			for (int[] columnGroup : columnGroupSet){
				columnGroups.add(columnGroup);
				size += columnGroup.length;
			}
		}
        
		int[] partitioning = new int[size];
		//TIntIntHashMap partitioning = new TIntIntHashMap();
		for (int idx = 0; idx < columnGroups.size(); idx++) {
			for (int column : columnGroups.get(idx)) {
				partitioning[column] = idx;
				//partitioning.put(column, idx);
			}
		}
		return partitioning;
		//return partitioning.values();
	}

	/**
	 * Method for creating a map of partitions from a non-overlapping
	 * partitioning.
	 */
	public static TIntObjectHashMap<TIntHashSet> getPartitioningMap(int[] partitioning) {

		TIntObjectHashMap<TIntHashSet> partitioningMap = new TIntObjectHashMap<TIntHashSet>();

		for (int a = 0; a < partitioning.length; a++) {

			if (!partitioningMap.containsKey(partitioning[a])) {
				partitioningMap.put(partitioning[a], new TIntHashSet(partitioning.length));
			}

			partitioningMap.get(partitioning[a]).add(a);
		}
		return partitioningMap;
	}

    /**
     * Method for converting a map-based representation of a partitioning to an array-based representation.
     * @param partitions The input in map-format.
     * @return The result in array format.
     */
    public static int[] getPartitioning(TIntObjectHashMap<TIntHashSet> partitions) {

        TIntIntHashMap partitioning = new TIntIntHashMap();
        for (int p : partitions.keys()) {
            for (int a: partitions.get(p).toArray()) {
                if (partitioning.containsKey(a)) {
                    throw new IllegalArgumentException("ERROR: The specified partitioning is overlapping!");
                }
                partitioning.put(a, p);
            }
        }

        int[] result = new int[partitioning.size()];
        for (int a : partitioning.keys()) {
            result[a] = partitioning.get(a);
        }

        return result;
    }

    /**
     * Method for converting a map-based representation of partitions to an array-based representation.
     * @param partitionsMap The input in map-format.
     * @return The result in array format.
     */
    public static int[][] getPartitions(TIntObjectHashMap<TIntHashSet> partitionsMap) {

        int[][] partitions = new int[partitionsMap.size()][];

        int minKey = Integer.MAX_VALUE;
        for (int p : partitionsMap.keys()) {
            if (p < minKey) {
                minKey = p;
            }
        }

        for (int p : partitionsMap.keys()) {
            // TODO: check if this hasn't changed any partitionings:
            partitions[p-minKey] = partitionsMap.get(p).toArray();
        }

        return partitions;
    }

	/**
	 * Method for creating a map of partitions from a possibly overlapping
	 * partitioning.
	 */
	public static TIntObjectHashMap<TIntHashSet> getPartitioningMap(HashSet<TIntHashSet> partitions) {
		TIntObjectHashMap<TIntHashSet> partitioningMap = new TIntObjectHashMap<TIntHashSet>();

		for (TIntHashSet partition : partitions) {
			partitioningMap.put(partitioningMap.size() + 1, partition);
            // TODO check if this wouldn't cause any changes in the partitionings of AUTOPART:
            //partitioningMap.put(partitioningMap.size(), partition);
		}

		return partitioningMap;
	}

	public static int[][] getNonNullVectors(int[][] usageM) {
		List<int[]> nonNullVectors = new ArrayList<int[]>();

		for (int[] v : usageM) {
			boolean isNull = true;
			for (int i = 0; i < v.length; i++)
				if (v[i] > 0)
					isNull = false;
			if (!isNull)
				nonNullVectors.add(v);
		}

		int[][] newUsageM = new int[nonNullVectors.size()][];
		for (int i = 0; i < newUsageM.length; i++) {
			newUsageM[i] = nonNullVectors.get(i);
		}

		return newUsageM;
	}

	public static int[][] transposeMatrix(int[][] usageM) {
		int[][] newUsageM = new int[usageM[0].length][usageM.length];
		for (int i = 0; i < usageM.length; i++) {
			for (int j = 0; j < usageM[i].length; j++)
				newUsageM[j][i] = usageM[i][j];
		}
		return newUsageM;
	}

	public static int[] getPartitioning(int[] splitVector, int[] ordering) {
		int[] partitioning = new int[splitVector.length];
		int currentPartitionId = 0;
		for (int i = 0; i < splitVector.length; i++) {
			partitioning[ordering[i]] = currentPartitionId;
			if (splitVector[i] == 1)
				currentPartitionId++;
		}
		return partitioning;
	}

    /**
     * Method for changing the partition IDs in a partitioning to form a consecutive series.
     * @param a The partitioning to be modified.
     * @return The modified partitioning.
     */
	public static int[] consecutivePartitionIds(int[] a) {
		Set<Integer> uniqueIds = new HashSet<Integer>();
		for (int element : a)
			uniqueIds.add(element);
		List<Integer> orderedIds = new ArrayList<Integer>(uniqueIds);
		Collections.sort(orderedIds);

		int[] new_a = new int[a.length];
		for (int i = 0; i < a.length; i++) {
			new_a[i] = orderedIds.indexOf(a[i]);
		}

		return new_a;
	}

	/**
	 * Method for creating a String representation of the partitions.
	 * 
	 * @param partitions
	 *            The map of the partitions.
	 * @return A String where the partitions are separated by a ',' and the
	 *         elements within by a '|' character.
	 */
	public static String partitioningString(TIntObjectHashMap<TIntHashSet> partitions) {

		StringBuilder sb = new StringBuilder();

        TreeMap<Integer, TreeSet<Integer>> orderedPartitions = new TreeMap<Integer, TreeSet<Integer>>();

		for (TIntIterator keyit = partitions.keySet().iterator(); keyit.hasNext();) {
			int key = keyit.next();
            TreeSet<Integer> partition = new TreeSet<Integer>();

			for (TIntIterator ait = partitions.get(key).iterator(); ait.hasNext();) {
				partition.add(ait.next());
			}

            orderedPartitions.put(partition.first(), partition);
		}

        for (TreeSet<Integer> partition : orderedPartitions.values()) {
            sb.append(partition);
        }

		return sb.toString();
	}

    /**
     * Method for creating a CREATE PROJECTION statement to create the vertical partitioned layout in the Vertica DBMS.
     *
     * @param partitions
     *            The map of the partitions.
     * @param projectionName
     *          The name of the table the projection is created for.
     * @param sourceTable
     *          The table object containing the attribute names.
     * @param sourceTableName
     *          The table the data will be loaded from.
     * @param encoding
     *          The encoding of the columns. Leave null for no encoding.
     * @return The CREATE PROJECTION statement.
     */
    public static String projectionString(TIntObjectHashMap<TIntHashSet> partitions, String projectionName,
                                           Table sourceTable, String sourceTableName, String encoding) {

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE PROJECTION ").append(projectionName).append(" (\n");

        int i_p = 0;
        for (int p : partitions.keys()) {
            int[] partition = partitions.get(p).toArray();
            i_p++;

            if (partition.length > 1) {
                sb.append("GROUPED(");
                for (int i_a = 0; i_a < partition.length; i_a++) {
                    int a = partition[i_a];
                    sb.append(sourceTable.attributes.get(a).name);
                    if (encoding != null) {
                        sb.append(" ENCODING ").append(encoding);
                    }
                    if (i_a < partition.length - 1) {
                        sb.append(", ");
                    }
                }
                sb.append(")");
            } else {
                int a = partition[0];
                sb.append(sourceTable.attributes.get(a).name);
                if (encoding != null) {
                    sb.append(" ENCODING ").append(encoding);
                }
            }


            if (i_p < partitions.size()) {
                sb.append(",\n");
            }
        }

        sb.append(")\n");
        sb.append("AS (SELECT *\n");
        sb.append("FROM ").append(sourceTableName).append('\n');
        sb.append("ORDER BY ").append(sourceTable.pk).append(");");

        return sb.toString();
    }
    
}
