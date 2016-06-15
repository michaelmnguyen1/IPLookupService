/*
 * Michael M. Nguyen
 */
package com.getcake.geo.dao;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStreamWriter;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import com.getcake.dao.BaseDao;
import com.getcake.geo.model.GeoInfo;
import com.getcake.geo.model.IpData;

public class GeoMsSqlDao extends BaseDao {

	private static GeoMsSqlDao instance;

	private String sqlGetLocationId, geoExportDir, sqlGetIspInfo;

	static final String IP_LOCATION_TABLE_NAME = "ipv6_city";
	static final String IP_LOCATION_TABLE_NAME_VER_PREFIX = IP_LOCATION_TABLE_NAME + "_";
	static final String IP_ISP_TABLE_NAME = "ipv6_isp";
	static final String IP_ISP_TABLE_NAME_PREFIX = IP_ISP_TABLE_NAME + "_";

	static final String LOCATION_TABLE_NAME = "cities";
	static final String ISP_TABLE_NAME = "isps";

	static final String EXPORT_FILE_SUFFIX = ".csv";


	static {
		try {
			instance = new GeoMsSqlDao();
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}

	public static GeoMsSqlDao getInstance() {
		return instance;
	}

	public void init(Properties properties) {
		this.properties = properties;
		dbJdbcUrl = properties.getProperty("geoExportSrcDbJdbcUrl");
		logger.debug("dbJdbcUrl: " + dbJdbcUrl);
		dbDriver = properties.getProperty("geoExportSrcDbDriver");
		properties.setProperty("user", properties.getProperty("geoExportSrclUser"));
		properties.setProperty("password", properties.getProperty("geoExportSrcPassword"));
		sqlGetLocationInfo = properties.getProperty("sqlGetLocationInfo");
		sqlGetIspInfo = properties.getProperty("sqlGetIspInfo");
	}

	public Date getLatestIpImportDate () {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		String respMsg = "";
		Date latestImportDate = null;

		try {
			logger.debug("");
			logger.debug("Started getLatestIpImportDate");
			startTime = Calendar.getInstance().getTimeInMillis();
			sourceConn = getConnection();
			sourceStmt = sourceConn.prepareStatement(
"select top 1 import_date from dataimports.dbo.imports where import_type_id = 5 order by import_date desc;");
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("exportGeoInfo sql execution (ms): "	+ (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F);
			if (sourceRs.next()) {
				latestImportDate = sourceRs.getTimestamp("import_date");
			}
			logger.debug("latestImportDate:" + latestImportDate);
		} catch (Throwable exc) {
			logger.error("", exc);
			throw new RuntimeException (exc);
		} finally {
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return latestImportDate;
	}

	public String exportGeoInfo (String outputFileName) {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		long count = 0;
		StringBuilder lineOutput;
		String respMsg = "";
		File outputFile;
		FileOutputStream fileOutputStream;
		BufferedWriter bufferedWriter = null;
		OutputStreamWriter outputStreamWriter;

		try {
			logger.debug("");
			logger.debug("Started exportGeoInfo for " + outputFileName);
			startTime = Calendar.getInstance().getTimeInMillis();
			sourceConn = getConnection();
			sourceStmt = sourceConn.prepareStatement(
"select location_id as locationId, country, region, city, latitude, longitude, metro_code as metroCode from cities order by locationId");
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("exportGeoInfo sql execution (ms): "	+ (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F);

			startTime = Calendar.getInstance().getTimeInMillis();
			/*
			outputFile = new File (outputFileName);
			if (outputFile.exists()) {
				if (!outputFile.delete()) {
					throw new RuntimeException ("Cannot delete " + outputFileName);
				}
			}
			outputFile.createNewFile();
			*/
	    	fileOutputStream = new FileOutputStream(outputFileName);

	    	outputStreamWriter = new OutputStreamWriter(fileOutputStream);
	    	bufferedWriter = new BufferedWriter (outputStreamWriter);
	    	lineOutput = new StringBuilder ();

			bufferedWriter.write("location_id,country,region,city,latitude,longitude,metro_code");
			bufferedWriter.write(System.getProperty("line.separator"));
			while (sourceRs.next()) {
				lineOutput.append(sourceRs.getInt("locationId"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("country"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("region"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("city"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getFloat("latitude"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getFloat("longitude"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("metroCode"));
				bufferedWriter.write(lineOutput.toString());
				lineOutput.delete(0, lineOutput.length());
				bufferedWriter.write(System.getProperty("line.separator"));
				count++;
			}
			bufferedWriter.flush();
			endTime = Calendar.getInstance().getTimeInMillis();
			respMsg = "exportGeoInfo dur(ms):" + (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F + " - # locations expoerted:" + count;
			logger.debug(respMsg);
		} catch (Throwable exc) {
			logger.error("", exc);
			throw new RuntimeException (exc);
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (Throwable exc) {
				logger.error("", exc);
			}
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return respMsg;
	}

	public String exportIspInfo (String outputFileName) {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		long count = 0;
		String respMsg = "";
		StringBuilder lineOutput;
		FileOutputStream fileOutputStream;
		BufferedWriter bufferedWriter = null;
		OutputStreamWriter outputStreamWriter;

		try {
			logger.debug("");
			logger.debug("Started exportIspInfo for " + outputFileName);
			startTime = Calendar.getInstance().getTimeInMillis();
			sourceConn = getConnection();
			sourceStmt = sourceConn.prepareStatement(
"select isp_id as ispId, provider_name as providerName from isps order by ispId");
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("exportIspInfo sql execution (ms): " + (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F);

			startTime = Calendar.getInstance().getTimeInMillis();

	    	fileOutputStream = new FileOutputStream(outputFileName);
	    	outputStreamWriter = new OutputStreamWriter(fileOutputStream);
	    	bufferedWriter = new BufferedWriter (outputStreamWriter);
	    	lineOutput = new StringBuilder ();

			bufferedWriter.write("isp_id,isp_provider_name");
			bufferedWriter.write(System.getProperty("line.separator"));

			while (sourceRs.next()) {
				lineOutput.append(sourceRs.getInt("ispId"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("providerName"));
				bufferedWriter.write(lineOutput.toString());
				lineOutput.delete(0, lineOutput.length());
				bufferedWriter.write(System.getProperty("line.separator"));
				count++;
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			respMsg = "exportIspInfo dur(ms):" + (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F + " - # ISPs loaded:" + count;
			logger.debug(respMsg);
		} catch (Throwable exc) {
			logger.error("", exc);
			throw new RuntimeException (exc);
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (Throwable exc) {
				logger.error("", exc);
			}
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return respMsg;
	}

	public long exportIpData(String tableName, String targetIdColName, String outputFileName) throws Throwable {
		long startTime, endTime;
		String logMsg, sql;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		long count = 0;
		String respMsg = "";
		StringBuilder lineOutput;
		FileOutputStream fileOutputStream;
		BufferedWriter bufferedWriter = null;
		OutputStreamWriter outputStreamWriter;

		try {
			logger.debug("");
			System.out.println("");
			logMsg = "=== start exportIpData for table: " + tableName + " - outputFileName: " + outputFileName;
			logger.debug(logMsg);
			System.out.println(logMsg);

			startTime = Calendar.getInstance().getTimeInMillis();
			sql = "select " + targetIdColName + " as targetId, ipv6_start, ipv6_end from " + tableName + " order by ipv6_start, ipv6_end";
			logger.debug(sql);
			System.out.println(sql);
			sourceConn = getConnection();
			sourceStmt = sourceConn.prepareStatement(sql);
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("exportIpData sql execution (ms): " + (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F);

			startTime = Calendar.getInstance().getTimeInMillis();
	    	fileOutputStream = new FileOutputStream(outputFileName);
	    	outputStreamWriter = new OutputStreamWriter(fileOutputStream);
	    	bufferedWriter = new BufferedWriter (outputStreamWriter);
	    	lineOutput = new StringBuilder ();

			bufferedWriter.write("targetId,ipv6_start,ipv6_end");
			bufferedWriter.write(System.getProperty("line.separator"));

			while (sourceRs.next()) {
				lineOutput.append(sourceRs.getInt("targetId"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("ipv6_start"));
				lineOutput.append(',');
				lineOutput.append(sourceRs.getString("ipv6_end"));
				bufferedWriter.write(lineOutput.toString());
				lineOutput.delete(0, lineOutput.length());
				bufferedWriter.write(System.getProperty("line.separator"));
				count++;

				if ((count % HashCacheDao.LOAD_LOG_INTERVAL) == 1) {
					endTime = Calendar.getInstance().getTimeInMillis();
					respMsg = "exportIpData Running check for " + tableName + " - dur(ms):" + (endTime - startTime)
						+ " - dur(minutes):" + (endTime - startTime) / 60000.0F + " - # IP loaded:" + count;
					logger.debug(respMsg);
				}
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			respMsg = "exportIpData for " + tableName + " - dur(ms):" + (endTime - startTime)
				+ " - dur(minutes):" + (endTime - startTime) / 60000.0F + " - # IP loaded:" + count;
			logger.debug(respMsg);
			System.out.println(respMsg);
		} catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
		} finally {
			try {
				if (bufferedWriter != null) {
					bufferedWriter.close();
				}
			} catch (Throwable exc) {
				logger.error("", exc);
			}
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return count;
	}

    public String exportMsSqlGeoData (String outputDirName, String geoFileName, String ipGeoRangeFileName, String ispFilename,
    	String ipIspRangeFileName) throws Throwable { // String exportGeoDataVer,

    	// outputFileName;
    	StringBuilder respMsg = new StringBuilder ();
    	File outputFile;
		long startTime, endTime;

		startTime = System.currentTimeMillis();

    	logger.debug("outputDirName: " + outputDirName);
		outputFile = new File (outputDirName);
		if (! outputFile.exists()) {
			// outputFile.delete();
			if (!outputFile.mkdirs()) {
				throw new RuntimeException ("failed to create output directory at " + outputDirName);
			}
		}

    	respMsg.append(this.exportGeoInfo (geoFileName));
		respMsg.append(System.getProperty("line.separator"));

    	respMsg.append(exportIpData(IP_LOCATION_TABLE_NAME, "location_id", ipGeoRangeFileName));
		respMsg.append(System.getProperty("line.separator"));

    	respMsg.append(this.exportIspInfo(ispFilename));
		respMsg.append(System.getProperty("line.separator"));

    	respMsg.append(exportIpData(IP_ISP_TABLE_NAME, "isp_id", ipIspRangeFileName));
		respMsg.append(System.getProperty("line.separator"));

		endTime = System.currentTimeMillis();
		respMsg.append(" Total export dur(ms):" + (endTime - startTime) + " - dur(minutes):" + (endTime - startTime) / 60000.0F);
    	logger.debug(respMsg.toString());

    	return respMsg.toString();
    }

	public void setSqlGetLocationId (String sqlGetLocationId) {
		this.sqlGetLocationId = sqlGetLocationId;
	}

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
			locationId = Integer.MAX_VALUE; // namedParameterJdbcTemplate.queryForInt(sqlGetLocationId, paramMap);
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("MS: ipv6: " + ipv6 + " dur(ms):" + (endTime - startTime));

			// getdata (ipv6, "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test", "root", "password");
			// getdata (ipv6, "jdbc:mysql://10.128.1.130:3306/_shared", "root", null);

		} catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
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

			geoInfoList = null; // namedParameterJdbcTemplate.queryForList(sqlGetLocationInfo, paramMap, GeoInfo.class);
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("MsSqlDao - loadLocationInfo duration(ms):" + (endTime - startTime));

			// getdata (ipv6, "jdbc:jtds:sqlserver://localhost:1433;DatanaseName=cfn_test", "root", "password");
			// getdata (ipv6, "jdbc:mysql://10.128.1.130:3306/_shared", "root", null);

		} catch (Throwable exc) {
			logger.error("", exc);
			throw new RuntimeException (exc);
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
			throw new RuntimeException (exc);
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
			logger.error("", exc);
			throw new RuntimeException (exc);
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
