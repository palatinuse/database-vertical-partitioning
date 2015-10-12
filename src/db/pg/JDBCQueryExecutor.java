package db.pg;


import db.schema.entity.Table;
import db.schema.entity.TableRow;
import db.schema.utils.TableRowUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;



/**
 * Query execution utility class
 * 
 * @author alekh
 *
 */
public class JDBCQueryExecutor extends QueryExecutor{

	JDBCConnectionManager connManager;
	Statement sql;       // statement to run queries with
	ResultSet lastRS;
	int lastRC;
	
	public static int CONN_RETRY = 5;
	
	
	
	
	public JDBCQueryExecutor(JDBCConnectionManager connManager){
		super();
		this.connManager = connManager;
		try {
			open(CONN_RETRY);
		} catch (SQLException e) {
			log("Error in connecting to database");
			throw new RuntimeException("Error in connecting to database:"+ e.getMessage());
		}
	}
	
	protected void open(int retryCount) throws SQLException{
		try {
			log("connection attempt="+(CONN_RETRY-retryCount+1));
			retryCount--;
			sql = connManager.getConnection().createStatement();
			//sql.setFetchSize(1000);
			//sql.setQueryTimeout(0);
		} catch (SQLException e) {
			if(retryCount > 0){
				try {
					log("Retrying connection to the database in "+(CONN_RETRY-retryCount+1)+" seconds");
					Thread.sleep(1000 * (CONN_RETRY-retryCount+1)); // sleep for few seconds
				} catch (InterruptedException e1) {
					// ignore
				}
				open(retryCount);
			}
			else{
				// failed to open connection
				log("Error in connecting to database");
				throw new RuntimeException("Error in connecting to database:"+ e.getMessage());
			}
		}
	}
	
	public void close(){
		try {
			log("closing database connection");
			sql.close();
			connManager.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	// execute string query
	public double execute(String query){
		try {
			if(sql.isClosed())
				open(CONN_RETRY);
			log("Executing query:"+query);

			t.reset();t.start();
			boolean hasResult = sql.execute(query);
			if(hasResult)
				lastRS = sql.getResultSet();
			t.stop();
			//close();
			
			return t.getElapsedTime();
			
		} catch (SQLException e) {
			log("Error in executing query "+query+":"+e.getMessage());
			throw new RuntimeException("Error in executing query: "+e.getMessage());
		}
	}
	
	public double executeAndIterate(String query){
		lastRC = 0;
		try {
			if(sql.isClosed())
				open(CONN_RETRY);
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
	
	public ResultSet getLastResultSet(){
		return this.lastRS;
	}
	
	// insert row in table
	public void insertRow(Table t, TableRow row){
		String attributeStr = t.getCSVAttributeList();
		String[] valueArray = new String[t.attributes.size()];
		
		StringBuffer valueStr = new StringBuffer();
		for(int i=0;i<valueArray.length;i++){
			valueStr.append("?");
			if(i < t.attributes.size()-1)
				valueStr.append(",");
		}
		
		String query = "INSERT INTO " + t.name + "(" + attributeStr +") VALUES(" + valueStr + ")";
		
		try {
			PreparedStatement ps = connManager.getConnection().prepareStatement(query);
			TableRowUtils.setValues(row, ps);
			ps.executeUpdate();
			ps.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
	public int getCount(){
		try {
			lastRS.next();
			return lastRS.getInt(1);
		} catch (SQLException e) {
			throw new RuntimeException("Error in getting the count: "+e.getMessage());
		}
	}
	
	protected void printResultSet(int numAttributes){
		try {
			lastRC = 0;
			while(lastRS.next()){
				for(int i=0;i<numAttributes;i++){
					//System.out.print(AttributeType.readAttribute(types[i], lastRS, i)+"\t");
					System.out.print(lastRS.getObject(i).toString()+"\t");
				}
				System.out.println();
				++lastRC;
			}
			System.out.println("\nNumber of Rows: "+ lastRC);
		} catch (SQLException e) {
			throw new RuntimeException("Error in printing result set: "+e.getMessage());
		}
	}
}
