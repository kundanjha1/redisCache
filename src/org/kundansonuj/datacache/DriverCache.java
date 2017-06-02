package org.kundansonuj.datacache;



import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

import org.kundansonuj.datacache.redis.JedisFactory;
import org.kundansonuj.datacache.redis.RedisClient;




public class DriverCache implements java.sql.Driver {

	private Driver wrappedDriver;
	String database;

	static {
		try {
			DriverManager.registerDriver(new DriverCache());
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	public boolean acceptsURL(String url) throws SQLException {
		String fixedUrl = fixupUrl(url);
		if (fixedUrl.equals(url)) {
			return false;
		}

		return wrappedDriver.acceptsURL(fixedUrl);
	}

	public Connection connect(String url, Properties info) throws SQLException {
		RedisClient redisClient = new RedisClient(url, info, new JedisFactory());
		// Remove our special stuff from the URL
		url = fixupUrl(url);

		// And pass through
		try {
			String driver=getDriverClassName(url);
			Class.forName(driver);

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		Connection conn=null;
		try{
		 conn =  DriverManager.getConnection(url,info);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		return new ConnectionCache(conn, redisClient,database);
	}

	private String getDriverClassName(String url) {
		 database =  url.split(":")[1];
		switch(database){
		case "vertica":
			return "com.vertica.jdbc.Driver";
		case "impala":
			return "com.cloudera.impala.jdbc41.Driver";
		case "mysql":
			return "com.mysql.jdbc.Driver";
		default:
			return null;


		}
	}

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
			throws SQLException {
		return wrappedDriver.getPropertyInfo(url, info);
	}

	public int getMajorVersion() {
		return wrappedDriver.getMajorVersion();
	}

	public int getMinorVersion() {
		return wrappedDriver.getMinorVersion();
	}

	public boolean jdbcCompliant() {
		return wrappedDriver.jdbcCompliant();
	}

	private String fixupUrl(String url) {
		url = url.replace("cache","");
		return url;
	}

	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return wrappedDriver.getParentLogger();
	}


}
