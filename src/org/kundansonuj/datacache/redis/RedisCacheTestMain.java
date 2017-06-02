package org.kundansonuj.datacache.redis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.kundansonuj.datacache.CachedDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisCacheTestMain {
	final private static Logger log = LoggerFactory.getLogger(RedisCacheTestMain.class);

	public static void main(String[] args) throws SQLException {
		Connection conn=CachedDatasource.getInstance().getConnection("vertica", 10);
		//PreparedStatement pst= conn.prepareStatement("Select count(1) from tbl_ads_request where day=16 and month=05");
		PreparedStatement pst= conn.prepareStatement("Select count(1) from tbl_statistics");
		ResultSet rs=pst.executeQuery();
		while(rs.next()){
			System.out.println(rs.getInt(1));	
		
		}
	}

}
