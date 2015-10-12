package db.pg;

import core.utils.PartitioningUtils;
import core.utils.TimeUtils.Timer;
import db.schema.entity.Table;
import db.schema.entity.TableRow;
import db.schema.utils.TableRowUtils;
import db.schema.utils.TableRowUtils.TableRowIterator;
import db.schema.utils.TableUtils;
import gnu.trove.iterator.TIntIterator;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Query execution utility class
 * 
 * @author alekh
 * 
 */
public abstract class QueryExecutor {

	// _TODO could we make this singleton?

	Timer t;

	boolean debug = false;
	DateFormat df = new SimpleDateFormat("yyyy:MM:dd HH:mm");

	public QueryExecutor() {
		t = new Timer();
	}

	public void debug() {
		this.debug = true;
	}

	public void nodebug() {
		this.debug = false;
	}

	public void log(String message) {
		if (debug) {
			System.out.println(df.format(new Date()) + " # " + message);
		}
	}

	// execute string query
	public abstract double execute(String query);

	public abstract int getCount();

	public abstract void close();

	public double executeNoResult(String query) {
		return execute("SELECT count(*) FROM (" + query + ") AS sub_query");
	}

	// create table
	public double createTable(Table t) {
		switch (t.type.getId()) {
		// standard table
		case TABLE:
			if (t.partitions == null)
				return execute("CREATE " + t.toString() + ";");
			else {
				double timeToCreate = 0;
				List<Table> partitionTables = TableUtils.perPartitionTables(t);
				for (Table partitionT : partitionTables)
					timeToCreate += execute("CREATE " + partitionT.toString() + ";");
				return timeToCreate;
			}

			// column grouped table
		case CTABLE:
			if (t.partitions == null)
				// c_segment_size=10,c_buffer_size=1,c_partitions="1,2|3"
				return execute("CREATE " + t.toString() + ";");
			else {
				// _TODO: create a CTable in the modified postgres
				return execute("CREATE " + t.toString() + " WITH (c_buffer_size=1000,c_partitions=\""
						+ PartitioningUtils.partitioningString(t.partitions) + "\");");
			}

			// invalid table type
		default:
			throw new UnsupportedOperationException("Unknown table type !");
		}
	}

	// bulk load
	public double loadTable(Table t, String dataFile, String delimiter) {
		switch (t.type.getId()) {
		case TABLE:
			if (t.partitions == null)
				return execute("COPY " + t.name + " FROM '" + dataFile + "' DELIMITER '" + delimiter + "'");
			else {
				this.t.reset();
				this.t.start();
				
				List<Table> partitionTables = TableUtils.perPartitionTables(t);

				TableRowIterator itr = new TableRowIterator(t, dataFile, delimiter);
				int count = 0;
				while (itr.hasNext()) {
					TableRow row = itr.next();
					for (TIntIterator keyit = t.partitions.keySet().iterator(); keyit.hasNext(); ) {
						int key = keyit.next();
						
						TableRow partialRow = TableRowUtils.partialTableRow(row, partitionTables.get(key), t.partitions.get(key).toArray());
						partialRow.add(count);
						insertRow(partitionTables.get(key), partialRow);
					}
					count++;
					if (count % 10000 == 0)
						log("Inserted " + count + " rows");
				}
				close();
				this.t.stop();
				return this.t.getElapsedTime();
			}
		case CTABLE:
			this.t.reset();
			this.t.start();
			TableRowIterator itr = new TableRowIterator(t, dataFile, delimiter);
			int count = 0;
			while (itr.hasNext()) {
				insertRow(t, itr.next());
				count++;
				if (count % 10000 == 0)
					log("Inserted " + count + " rows");
			}
			this.t.stop();
			return this.t.getElapsedTime();
		default:
			throw new UnsupportedOperationException("Unknown table type !");
		}
	}

	// insert row in table
	public abstract void insertRow(Table t, TableRow row);

	// execute drop table query
	public void dropTable(Table t) {
		switch (t.type.getId()) {
		case TABLE:
			if (t.partitions == null)
				execute("DROP TABLE IF EXISTS " + t.name);
			else {
				List<Table> partitionTables = TableUtils.perPartitionTables(t);
				for (Table pT : partitionTables)
					execute("DROP TABLE IF EXISTS " + pT.name);
			}
			break;

		case CTABLE:
			execute("DROP CTABLE IF EXISTS " + t.name);
			break;
		default:
			throw new UnsupportedOperationException("Unknown table type !");
		}
	}

	// get all databases
	public void showDatabases() {
		execute("select datname from pg_database");
		printResultSet(1);
	}

	// get all tables
	public void showTables() {
		execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
		printResultSet(1);
	}

	// describe table
	public void tableColumns() {
		execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema = 'public'");
		printResultSet(2);
	}

	public void tableColumns(String tn) {
		execute("SELECT column_name FROM information_schema.columns WHERE table_name='" + tn + "'");
		printResultSet(1);
	}

	// describe table
	public void tableComment(String tn) {
		execute("SELECT table_comment FROM information_schema.tables WHERE table_name = '" + tn + "'");
		printResultSet(1);
	}

	public void tableRows(String tn) {
		execute("SELECT COUNT(A0) FROM " + tn);
		printResultSet(1);
	}

	public void tableRows(String tn, String attr) {
		execute("SELECT COUNT(" + attr + ") FROM " + tn);
		printResultSet(1);
	}

	protected abstract void printResultSet(int numAttributes);

	/* Copy-paste programming: TimeUtils.Timer!!! */
	/*
	public static class Timer {
		private long startTime;
		private double elapsedTime;

		public void start() {
			startTime = System.nanoTime();
		}

		public void stop() {
			elapsedTime += (double) (System.nanoTime() - startTime) / 1E9;
			startTime = System.nanoTime();
		}

		public void reset() {
			elapsedTime = 0;
		}

		public double getElapsedTime() {
			return elapsedTime;
		}
	} */
}
