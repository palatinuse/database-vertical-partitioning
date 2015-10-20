package db.schema.utils;

import db.schema.entity.Query;
import db.schema.entity.RangeQuery;
import db.schema.entity.Table;
import gnu.trove.iterator.TIntIterator;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.set.hash.TIntHashSet;

public class QueryUtils {

	/**
	 * Method for producing the SELECT clause of the SQL query.
	 * 
	 * @param t
	 *            The table being queried.
	 * @param refAttributes
	 *            The attributes being projected.
	 * @return The SELECT clause.
	 */
	public static String getProjectionString(Table t, int[] refAttributes) {
		String prj = "";
		for (int i = 0; i < refAttributes.length; i++) {
			prj += t.attributes.get(refAttributes[i]).name;
			if (i < refAttributes.length - 1)
				prj += ",";
		}
		return prj;
	}

	/**
	 * Method for producing the FROM clause of the SQL query.
	 * 
	 * It chooses partitions that contain the referenced attributes, if the
	 * table is partitioned. In case of overlapping partitions it can take into
	 * account a partition-selection plan, too.
	 * 
	 * @param t
	 *            The table being queried.
	 * @param refAttributes
	 *            The attributes being referenced.
	 * @param bestSolution
	 *            The partition-selection plan.
	 * @return The FROM clause.
	 */
	public static String getSourceTableString(Table t, int[] refAttributes, TIntHashSet bestSolution) {

		if (t.partitions == null)
			return t.name;

		else {

			TIntHashSet refPartitions = getRefPartitions(t, refAttributes, bestSolution);

			StringBuilder sb = new StringBuilder();

			for (TIntIterator pit = refPartitions.iterator(); pit.hasNext();) {
				sb.append(t.name).append('_').append(pit.next());

				if (pit.hasNext()) {
					sb.append(',');
				}
			}

			return sb.toString();
		}
	}

	/**
	 * Method for producing the WHERE clause of the SQL query for join conditions.
	 * 
	 * It chooses partitions that contain the referenced attributes, if the
	 * table is partitioned. In case of overlapping partitions it can take into
	 * account a partition-selection plan, too.
	 * 
	 * @param t
	 *            The table being queried.
	 * @param refAttributes
	 *            The attributes being referenced.
	 * @param bestSolution
	 *            The partition-selection plan.
	 * @return The WHERE clause for the join conditions.
	 */
	public static String getJoinConditionString(Table t, int[] refAttributes, TIntHashSet bestSolution) {

		if (t.partitions == null) {
			return null;
		} else {

			TIntHashSet refPartitions = getRefPartitions(t, refAttributes, bestSolution);

			StringBuilder sb = new StringBuilder();

			TIntArrayList refPartitionList = new TIntArrayList(refPartitions);

			for (int i = 0; i < refPartitionList.size() - 1; i++) {
				sb.append(t.name).append('_').append(refPartitionList.get(i)).append(".rid=").append(t.name)
						.append('_').append(refPartitionList.get(i + 1)).append(".rid");
				
				if (i < refPartitionList.size() - 2)
					sb.append(" AND ");
			}

			return sb.toString();
		}
	}

	/**
	 * Method for generating the set of referenced partitions by finding the
	 * necessary partitions for the referenced attributes. In case of
	 * overlapping partitions a partition-selection plan should be provided. If
	 * it is not provided, the first partition containing each attribute will be
	 * selected.
	 */
	private static TIntHashSet getRefPartitions(Table t, int[] refAttributes, TIntHashSet bestSolution) {
		TIntHashSet refPartitions = new TIntHashSet();

		if (bestSolution == null) {

			/*
			 * Find the first partition that contains each referenced attribute.
			 */
			for (int a : refAttributes) {
				for (TIntIterator pit = t.partitions.keySet().iterator(); pit.hasNext();) {
					int p = pit.next();

					if (t.partitions.get(p).contains(a)) {
						refPartitions.add(p);
						break;
					}
				}
			}

		} else {

			/*
			 * Use the partition selection plan to find a partition that
			 * contains each referenced attribute.
			 */
			refPartitions.addAll(bestSolution.toArray());
		}

		return refPartitions;
	}

    /**
     * Method for creating a partial query, which is query projecting only a subset of the attributes
     * projected by a source query.
     * @param q the source query, to be reduced
     * @param attributeMapping the mapping of the attribute IDs between the source- and the target queries.
     * @return the partial query
     */
	public static Query getPartialQuery(Query q, TIntIntHashMap attributeMapping){

		Query partialQuery = new Query(q);

        if (attributeMapping != null) {

            TIntArrayList partialQueryAttributes = new TIntArrayList();
            int[] projections = q.getProjectedColumns();

            for (int p : projections) {
                partialQueryAttributes.add(attributeMapping.get(p));
            }

            partialQuery.setProjections(attributeMapping.size(), partialQueryAttributes.toArray());
        }
		
		return partialQuery;
	}

    /**
     * Method for creating a partial query, which is query projecting only a subset of the attributes
     * projected by a source query.
     * @param q the source query, to be reduced
     * @param attributeMapping the mapping of the attribute IDs between the source- and the target queries.
     * @return the partial query
     */
    public static RangeQuery getPartialQuery(RangeQuery q, TIntIntHashMap attributeMapping){

        return new RangeQuery(getPartialQuery(q, attributeMapping), q.getRangeCondition());
    }
}
