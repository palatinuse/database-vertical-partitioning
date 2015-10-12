package db.pg;

import java.sql.SQLException;





public class InternalJDBCQueryExecutor extends JDBCQueryExecutor{

	public InternalJDBCQueryExecutor(JDBCConnectionManager connManager) {
		super(connManager);
	}

	// execute string query
	public double execute(String query){
		try {
			log("Executing query:"+query);
			t.reset();t.start();
			boolean hasResult = sql.execute(query);
			if(hasResult)
				lastRS = sql.getResultSet();
			t.stop();
			close();
			
			return t.getElapsedTime();
			
		} catch (SQLException e) {
			log("Error in executing query "+query+":"+e.getMessage());
			throw new RuntimeException("Error in executing query: "+e.getMessage());
		}
	}
	
	public double executeAndIterate(String query){
		lastRC = 0;
		try {
			log("Executing query:"+query);
			t.reset();t.start();
			boolean hasResult = sql.execute(query);
			if(hasResult){
				lastRS = sql.getResultSet();
				while(lastRS.next())
					++lastRC;
			}
			t.stop();	
			lastRS.close();
			close();
			
			return t.getElapsedTime();
		} catch (SQLException e) {
			log("Error in executing query "+query+":"+e.getMessage());
			throw new RuntimeException("Error in executing query: "+e.getMessage());
		}
	}
}
