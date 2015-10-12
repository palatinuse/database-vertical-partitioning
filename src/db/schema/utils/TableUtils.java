package db.schema.utils;

import core.utils.ArrayUtils;
import db.schema.BenchmarkTables;
import db.schema.entity.Attribute;
import db.schema.entity.Table;
import db.schema.types.AttributeType;
import gnu.trove.iterator.TIntIterator;

import java.util.ArrayList;
import java.util.List;

public class TableUtils {

	public static List<Table> perPartitionTables(Table t) {
		
		int[] queries = ArrayUtils.simpleArray(t.workload.queries.size(), 0, 1);
		List<Table> partitionTables = new ArrayList<Table>();
		
		for (TIntIterator keyit = t.partitions.keySet().iterator(); keyit.hasNext(); ) {
			int key = keyit.next();
			
			Table partitionT = BenchmarkTables.partialTable(t, t.partitions.get(key).toArray(), queries);
			partitionT.name = t.name + "_" + key;
			// add rid attribute
			partitionT.attributes.add(new Attribute("rid", AttributeType.Integer()));
			partitionTables.add(partitionT);
		}

		return partitionTables;
	}
}
