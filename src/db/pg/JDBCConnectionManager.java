package db.pg;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * PostgreSQL connection manager
 * 
 * @author alekh
 *
 */
public class JDBCConnectionManager {

	private String database;
	private String username;
	private String password;
	private String hostport;
	protected Connection db;
	
	private String hostname = null;
	private int portnumber = 0;
	
	public static JDBCConnectionManager getConnectionManager(String host, String db, String user, String pwd){
		JDBCConnectionManager connManager = new JDBCConnectionManager(user, pwd, db);
		connManager.setHostName(host);
		return connManager;
	}
	
	public JDBCConnectionManager(String username, String password, String database){
		this.username = username;
		this.password = password;
		this.database = database;
		this.hostport = "";
	}
	
	public void setHostName(String hostname){
		this.hostname = hostname;
	}
	
	public void setPortNumber(int portnumber){
		this.portnumber = portnumber;
	}
	
	public void close(){
		try {
			if(db!=null && !db.isClosed())
				db.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public Connection getConnection(){
		if(hostname!=null){
			hostport = "//"+hostname;
			if(portnumber!=0)
				hostport += ":" + portnumber;
			hostport += "/";
		}
		
		try {
			// check if connection already open
			if(db!=null && !db.isClosed())
				return db;
			
			// load the JDBC driver for PostgreSQL
			Class.forName("org.postgresql.Driver");
			
			// connect to the datbase server over TCP/IP 
			// (requires that you edit pg_hba.conf 
			// as shown in the "Authentication" section of this article)
			db = DriverManager.getConnection("jdbc:postgresql:"+ hostport + database,
					username,
					password);
			
			return db;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} 

		return null;
	}
}
