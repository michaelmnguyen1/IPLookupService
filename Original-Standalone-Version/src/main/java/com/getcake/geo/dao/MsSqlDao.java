package com.getcake.geo.dao;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;

import com.getcake.geo.model.GeoInfo;
import com.getcake.geo.model.IpData;

public class MsSqlDao extends BaseDao {

	private String sqlGetLocationId;
	
	public void setSqlGetLocationId (String sqlGetLocationId) {
		this.sqlGetLocationId = sqlGetLocationId;
	}
		
	// @Autowired
	private String sqlGetLocationInfo;
	public void setSqlGetLocationInfo (String sqlGetLocationInfo) {
		this.sqlGetLocationInfo = sqlGetLocationInfo;
	}
	
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int findLocationId(BigInteger ipv4, String ipv6){
		
		int locationId = 0;	
		List<IpData> objs;
		byte [] bytesutf8;
		Map<String, Object> paramMap;
		long startTime, endTime;
		
		try {
			paramMap = new HashMap<String, Object>(1);			
			bytesutf8 = hexStringToByteArray (ipv6); // ipv6.getBytes(UTF8); // new byte []{ (byte)0xFF, (byte)0x00} ;  // " + ipv6 + "
			paramMap.put("ipv6", bytesutf8);
			
			// getdata (ipv6, "jdbc:jtds:sqlserver://CA1-DEV-PRD-DB1:1433;DatabaseName=_shared;Instance=SQL2012", "mnguyen", "Frontier!23");
			// "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test"
			
			// logger.debug("findLocationId: bytes conversion ipv6 bytes: " + bytesutf8);
			startTime = Calendar.getInstance().getTimeInMillis();
			locationId = namedParameterJdbcTemplate.queryForInt(sqlGetLocationId, paramMap);			
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("MS: ipv6: " + ipv6 + " dur(ms):" + (endTime - startTime));

			// getdata (ipv6, "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test", "root", "password");
			// getdata (ipv6, "jdbc:mysql://10.128.1.130:3306/_shared", "root", null);
			
		} catch (Throwable exc) {
			logger.error("", exc);			
		}
		return locationId;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public List<GeoInfo> loadLocationInfo (){
		
		List<GeoInfo> geoInfoList = null;
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement stmt;
		Connection conn;
		
		try {
			// logger.debug("findLocationId: bytes conversion ipv6 bytes: " + bytesutf8);
			startTime = Calendar.getInstance().getTimeInMillis();
			
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		    conn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);					
			stmt = conn.prepareStatement("select * from  ipv6_city_A  where ? <= ipv6_end and ?  >= ipv6_start order by ipv6_end");
			
			geoInfoList = namedParameterJdbcTemplate.queryForList(sqlGetLocationInfo, paramMap, GeoInfo.class);			
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("MsSqlDao - loadLocationInfo duration(ms):" + (endTime - startTime));

			// getdata (ipv6, "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test", "root", "password");
			// getdata (ipv6, "jdbc:mysql://10.128.1.130:3306/_shared", "root", null);
			
		} catch (Throwable exc) {
			logger.error("", exc);			
		}
		return geoInfoList;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int transferIpv6Lookup (DataSource dstDataSource, int lowerImportId, int upperImportId){
		
		List<GeoInfo> geoInfoList;
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0;
		
		try {
			// logger.debug("findLocationId: bytes conversion ipv6 bytes: " + bytesutf8);
			startTime = Calendar.getInstance().getTimeInMillis();
			
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		    sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);		    
			sourceStmt = sourceConn.prepareStatement(
				"select * from  ipv6_city_A   (nolock) where location_id >= ? and location_id  <= ? order by location_id desc" );
			sourceStmt.setInt(1, lowerImportId);
			sourceStmt.setInt(2, upperImportId);
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			
			destConn = dstDataSource.getConnection();
			insertStmt = destConn.prepareStatement(
"insert into  _shared.ipv6_city_A (import_id , ipv6_start, ipv6_end, location_id) values (?, ?, ?, ?)");			
    		
			startTime = Calendar.getInstance().getTimeInMillis();
			while (sourceRs.next()) {
				
				if ((count % 1000) == 0) {
					logger.debug ("transferIpv6Lookup- count:" + count + " - import_id:" + sourceRs.getInt("import_id") +   
							" location_id:" + sourceRs.getInt("location_id") + 
							" - ipv6_start: " + sourceRs.getBytes("ipv6_start") + 
							" - ipv6_end: " + sourceRs.getBytes("ipv6_end")); 					
				}
				// 
				//		" - location_id:" + rs.getInt("location_id") + " - ipv6_start:" + strbytes + " - ipv6_end:" + strbytes_end);
				insertStmt.setInt(1, sourceRs.getInt("import_id"));
				insertStmt.setBytes(2, sourceRs.getBytes("ipv6_start"));
				insertStmt.setBytes(3, sourceRs.getBytes("ipv6_end"));
				insertStmt.setInt(4, sourceRs.getInt("location_id"));
				try {
					if (1 != insertStmt.executeUpdate()) {
						/*
						logger.error ("transferIpv6Lookup- insert failed for" + " import_id:" + sourceRs.getInt("import_id") +   
								" location_id:" + sourceRs.getInt("location_id") + 
								" - ipv6_start: " + sourceRs.getBytes("ipv6_start") + 
								" - ipv6_end: " + sourceRs.getBytes("ipv6_end"));
					    */ 					
					} else {
						count++;						
					}
				} catch (Throwable exc) {
					/*
					logger.error ("transferIpv6Lookup- insert failed for" + " import_id:" + sourceRs.getInt("import_id") +   
							" location_id:" + sourceRs.getInt("location_id") + 
							" - ipv6_start: " + sourceRs.getBytes("ipv6_start") + 
							" - ipv6_end: " + sourceRs.getBytes("ipv6_end"));
					*/ 										
				}
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			
			logger.debug("MsSqlDao - transferIpv6Lookup insert duration(ms):" + (endTime - startTime));

			
		} catch (Throwable exc) {
			logger.error("", exc);			
		}
		return count;
	}
	
	public void getdata (String ipv6, String url, String username, String password) {
		Connection conn = null;

		Blob blob;
		InputStream binStream;
		ByteArrayInputStream bis; 
		Charset utf8charset;
		byte [] bytesutf8 = { 3, 0, 0, 0}, bytes ;
		PreparedStatement stmt;
		
		try {
			// Class.forName("com.mysql.jdbc.Driver");
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
		    conn = DriverManager.getConnection(url, username, password);					
		    
		   System.out.println ("");				
		   System.out.println ("setBinaryStream url:" + url + " - ipv6:" + ipv6);
		   Charset UTF8   = Charset.forName("UTF-8");
		   // bytesutf8 = ipv6.getBytes(UTF8);
		   // bis = new ByteArrayInputStream (bytesutf8);
		   conn.setAutoCommit(true);
		   
		   bytesutf8 = hexStringToByteArray (ipv6); // ipv6.getBytes(UTF8); // new byte []{ (byte)0xFF, (byte)0x00} ;  // " + ipv6 + "
		   byte [] bytesutf8_end = bytesutf8; // new byte []{ (byte)0xFF, (byte)0xFF} ;  // " + ipv6 + "
		   		   
		   /*
		   stmt = conn.prepareStatement("insert into  ipv6lookup (id, ipv6_start, ipv6_end, location_id) values (10, ?, ?, 1000)");
		   stmt.setBytes(1, bytesutf8);
		   stmt.setBytes(2, bytesutf8_end);
		   int numRows = stmt.executeUpdate();
		   System.out.println ("numRows:" + numRows);				
		   */
		   
		   stmt = conn.prepareStatement("select * from  ipv6_city_A  where ? <= ipv6_end and ?  >= ipv6_start order by ipv6_end");
		   // bytesutf8 = new byte []{ 0x45, 0x67} ;  // " + ipv6 + "
		   System.out.println ("bytes:" + bytesutf8);				
		   stmt.setBytes(1, bytesutf8);// binStream = new InputStream ();
		   stmt.setBytes(2, bytesutf8);// binStream = new InputStream ();
		   ResultSet rs = stmt.executeQuery();
		   mapRow (rs, 0, ipv6);
		   
		} catch (Throwable exc) {
			
			exc.printStackTrace();
			// conn.close();
		}
	}
	
	public Object mapRow(ResultSet rs, int rowNum, String ipv6) throws SQLException {

		   Charset UTF8   = Charset.forName("UTF-8");
		   /*
		_rsmd = rs.getMetaData();
		
		for (int i = 1; i <= _rsmd.getColumnCount(); i++) {
			Object obj = _rsmd.getColumnClassName(i);
			System.out.println ("col class: " + _rsmd.getColumnName(i)); 			 
			System.out.println ("col class: " + obj); 			 
		}
		*/
		   
		while (rs.next()) {
			// String strbytes = rs.getString("ipv6_start");
			// String strbytes_end = rs.getString("ipv6_end");
			System.out.println ("ipv6:" + ipv6 + " - location_id:" + rs.getInt("location_id"));
			// " - import_id:" + rs.getInt("import_id") + 
			//		" - location_id:" + rs.getInt("location_id") + " - ipv6_start:" + strbytes + " - ipv6_end:" + strbytes_end);
			
		}
		return null;
	}
 		
	
}
