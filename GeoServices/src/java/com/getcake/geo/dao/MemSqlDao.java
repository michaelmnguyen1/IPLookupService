package com.getcake.geo.dao;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class MemSqlDao extends BaseDao {

	public int findLocationId(BigInteger ipv4, String ipv6){
		
		int locationId = 0;	
		byte [] bytesutf8;
		Map<String, Object> paramMap;
		long startTime, endTime;
		
		try {
			paramMap = new HashMap<String, Object>(1);			
			bytesutf8 = hexStringToByteArray (ipv6); // ipv6.getBytes(UTF8); // new byte []{ (byte)0xFF, (byte)0x00} ;  // " + ipv6 + "
			paramMap.put("ipv6", bytesutf8);
			
			// logger.debug("findLocationId: bytes conversion ipv6 bytes: " + bytesutf8);
			startTime = Calendar.getInstance().getTimeInMillis();
			locationId = Integer.MAX_VALUE; // namedParameterJdbcTemplate.queryForInt(sqlGetLocationId, paramMap);			
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("MemSql: ipv6:" + ipv6 + " dur(ms):" + (endTime - startTime));

			// getdata (ipv6, "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test", "root", "password");
			// getdata (ipv6, "jdbc:mysql://10.128.1.130:3306/_shared", "root", null);
			
		} catch (Throwable exc) {
			logger.error("", exc);			
		}
		return locationId;
	}

	private PreparedStatement stmt = null;
	
	public int findLocationIdDirect (String ipv6) {
		Connection conn = null;

		int locationId = 0;	
		byte [] bytesutf8;
		ResultSet rs = null;
		long startTime, endTime;
		
		try {
			if (stmt == null) {
			    conn = this.dataSource.getConnection();					
				conn.setAutoCommit(true);
				stmt = conn.prepareStatement("select location_id from  _shared.ipv6_city_A  where ? <= ipv6_end and ?  >= ipv6_start order by ipv6_end limit 1");
			}
		    
		   bytesutf8 = hexStringToByteArray (ipv6);		   		   
		   stmt.setBytes(1, bytesutf8);
		   stmt.setBytes(2, bytesutf8);
		   startTime = Calendar.getInstance().getTimeInMillis();
		   rs = stmt.executeQuery();
		   endTime = Calendar.getInstance().getTimeInMillis();
		   logger.debug("MemSql-Direct: ipv6:" + ipv6 + " dur(ms):" + (endTime - startTime));
		   if (rs.next()) {
			   locationId = rs.getInt(1);
		   }
		   
		} catch (Throwable exc) {			
			exc.printStackTrace();
		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				/*
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
				*/
			} catch (Throwable exc) {
				logger.error("err closing db resources", exc);
			}			
		}
		return locationId;
	}
	
	
	public class CustomerRowMapper // implements RowMapper
	{
		public Object mapRow(ResultSet rs, int rowNum) throws SQLException {

			byte [] bytes = rs.getBytes(1);
			Blob blob = rs.getBlob(1);

			System.out.println (bytes.length);
	        bytes = blob.getBytes(1, (int) blob.length());
	        
			InputStream ipstr = rs.getBinaryStream(1);
			
			System.out.println (blob.toString());
			System.out.println (blob.length());
			bytes = blob.getBytes(1, (int)blob.length());
			System.out.println (bytes.length);
			
			for (int i = 0; i <  bytes.length; i++) {
				System.out.println (bytes[i]);				
			}
			
			System.out.println (ipstr);
			
			return blob;
		}
	 
	}	
}
