package org.kundansonuj.datacache;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 
 * @author tr-dt-043
 *
 */
public class CachedDatasource {


	protected static String serverName=PropertiesManager.getInstance().getProperty("serverName").trim();
	protected static int portNumber=Integer.parseInt(PropertiesManager.getInstance().getProperty("portNumber").trim());
	protected static String databaseName=PropertiesManager.getInstance().getProperty("databaseName").trim();
	protected static String user=PropertiesManager.getInstance().getProperty("user").trim();
	protected static String password=PropertiesManager.getInstance().getProperty("password").trim();
	protected static String maxPooledConnections=PropertiesManager.getInstance().getProperty("maxPooledConnections").trim();
	protected static String impalaServer=PropertiesManager.getInstance().getProperty("impalaServer").trim();
	protected static String impalaPort=PropertiesManager.getInstance().getProperty("impalaPort").trim();
	protected static String redisUrl=PropertiesManager.getInstance().getProperty("redisUrl").trim();

	static Connection conn=null;
	int timeout=60;

	private static CachedDatasource instance=new CachedDatasource();

	final static String VERTICA="jdbc:verticacache://"+serverName+":5433/vertozdb";
	final static String IMPALA="jdbc:impalacache://"+impalaServer+":"+impalaPort+"/vertoz";
	final static String MYSQL="";
	static String url=null;

	private CachedDatasource(){

	}
	public  Connection getConnection(String database,int timeout){
		switch (database){
		case "vertica":
			url=VERTICA;
			break;
		case "impala":
			url=IMPALA;
			break;
		case "mysql":
			url=MYSQL;
			break;
		default:
			System.out.println("DATABASE NOT SELECTED! use getConnection(databse<name of database e.g vertica or impala or mysql>, timeout <No of seconds data to stored in cache>");


		}
		if(timeout!=0)
			this.timeout=timeout;

		return databaseConnection(database,url,this.timeout);
	}

	private  Connection databaseConnection(String database,String url, int timeout) {

		Connection con = null;
		try {
			/**@link http://tomcat.apache.org/tomcat-7.0-doc/jndi-datasource-examples-howto.html
			 * the web applications that have database drivers in their WEB-INF/lib directory cannot
			 *  rely on the service provider mechanism and should register the drivers explicitly.
			 */
			// Explicitly Loading Drivers
			Class.forName("com.vertica.jdbc.Driver");
			Class.forName("com.cloudera.impala.jdbc41.Driver");
			Class.forName("com.mysql.jdbc.Driver");
			Class.forName("com.vertoz.report.api.datacache.DriverCache");
			Properties myProp = new Properties();
			myProp.put("redisUrl", redisUrl);
			myProp.put("user", user);
			myProp.put("password", password);
			myProp.put("Label", "ReportAPIConnection");
			myProp.put("redisExpiration",String.valueOf(timeout));
			con = DriverManager.getConnection(url,myProp);


		}
		catch (Exception e) {
			e.printStackTrace();	
		}
		return con;
	}


	public void freeConnection(Connection con){
		if(con!=null)
			try {

				con.close();

			} catch (SQLException e) {

				e.printStackTrace();
			} 

	}

	public static CachedDatasource getInstance() {
		return instance;
	}	


}