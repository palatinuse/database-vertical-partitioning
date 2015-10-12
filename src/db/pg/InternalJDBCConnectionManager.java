package db.pg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;




public class InternalJDBCConnectionManager extends JDBCConnectionManager{

	public InternalJDBCConnectionManager() {
		super(null,null,null);
	}

	public void close(){
		try {
			if(db!=null)
				db.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public Connection getConnection(){
		try {
			if(db!=null)
				return db;
			
			db = DriverManager.getConnection("jdbc:default:connection");
			return db;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
}
