package db.pg;

import db.schema.entity.Table;
import db.schema.entity.TableRow;

public class PSQLQueryExecutor extends QueryExecutor{
	
	@Override
	public double execute(String query) {
		// _TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void insertRow(Table t, TableRow row) {
		throw new UnsupportedOperationException("Currently, we do not allow per-row insert with psql; use loadTable instead");
	}

	@Override
	protected void printResultSet(int numAttributes) {
		// _TODO Auto-generated method stub
	}

	@Override
	public void close() {
		// _TODO Auto-generated method stub
	}

	@Override
	public int getCount() {
		// _TODO Auto-generated method stub
		return 0;
	}
}
