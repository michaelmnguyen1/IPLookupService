package com.getcake.geo.dao;

import java.math.BigInteger;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;

import com.getcake.geo.model.*;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import test.HostAndPortUtil;

public class HashCacheDao extends BaseDao {

	private static HostAndPort hnp = HostAndPortUtil.getRedisServers().get(0);
	private static int IPV4_MAX_TOP_LEVEL_CACHE_SIZE = 10_000_000; 
	private static int IPV4_4BYTES_CACHE_SIZE = 1_100_000; 
	private static int LOAD_LOG_INTERVAL = 1_000_000;

	private static int IPV6_8BYTES_CACHE_SIZE = 220_000; 
	private static int GEO_INFO_CACHE_SIZE = 200_000; 
	
	private String sqlGetIpLookup;

	private String sqlGetLocationInfo;

	private Ipv4Cache ipv4Cache = null;
	private Ipv6Cache ipv6Cache = null;
	
	public HashCacheDao () {
	}

	public void setSqlGetIpLookup (String sqlGetIpLookup) {
		this.sqlGetIpLookup = sqlGetIpLookup;
	}

	public void setSqlGetLocationInfo (String sqlGetLocationInfo) {
		this.sqlGetLocationInfo = sqlGetLocationInfo;
	}

	public long loadCache (boolean flushCacheFlag, long topNumRows) throws Throwable {
		long count = loadCacheIpv6 (flushCacheFlag, topNumRows);
		return count += loadCacheIpv4 (flushCacheFlag, topNumRows);
	}
	
	public long loadCacheIpv6 (boolean flushCacheFlag, long topNumRows) {
		
		int numBytes = 16;
		long startTime, endTime, locationId;
		startTime = Calendar.getInstance().getTimeInMillis();
		
		if (ipv6Cache == null) {
			ipv6Cache = new Ipv6Cache ();
		}
		
		if (ipv6Cache.subBytesCache == null) {
			ipv6Cache.subBytesCache = new HashMap<BigInteger, Ipv6RangeNode>(IPV6_8BYTES_CACHE_SIZE);
		} else if (flushCacheFlag) {
			ipv6Cache.subBytesCache.clear();
		}

		int accCount = 0;
		// To do: dynamically determine max number of matching bytes for ipv6.  For now use static analysis which 
		// comes out to be 9.
		for (int startByteNum = 1; startByteNum < 9; startByteNum++) {
			accCount += loadCacheIpv6FirstNByte (ipv6Cache, flushCacheFlag, startByteNum, numBytes, topNumRows);
		}

		endTime = Calendar.getInstance().getTimeInMillis();    	
		logger.debug("ipv6 loaded dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipv6Cache.numNodes +  
				" - MaxNodeLength:" + ipv6Cache.maxNodeLength + " - ipvipCache.NodeLength:" + ipv6Cache.minNodeLength +
				" - MaxNodeIpStart: " + ipv6Cache.maxNodeIpStart + " - ipv6IpCache.maxNodeIpEnd: " + ipv6Cache.maxNodeIpEnd +
				" - MinNodeIpStart: " + ipv6Cache.minNodeIpStart + " - ipv6IpCache.minNodeIpEnd: " + ipv6Cache.minNodeIpEnd);
    	
		return ipv6Cache.numNodes;
	}
		
	public long loadCacheIpv4 (boolean flushCacheFlag, long topNumRows) throws Throwable  {
		
		int numBytes = 4;
		long startTime, endTime, locationId;
		startTime = Calendar.getInstance().getTimeInMillis();
		
		if (ipv4Cache == null) {
			ipv4Cache = new Ipv4Cache ();
		}
		
		if (ipv4Cache.subBytesCache == null) {
			ipv4Cache.subBytesCache = new HashMap<Long, Ipv4RangeNode>(IPV4_MAX_TOP_LEVEL_CACHE_SIZE);
		} else if (flushCacheFlag) {
			ipv4Cache.subBytesCache.clear();
		}

		if (ipv4Cache.fullBytesCache == null) {
			ipv4Cache.fullBytesCache = new HashMap<Long, Integer>(IPV4_4BYTES_CACHE_SIZE);				
		} else if (flushCacheFlag) {
			ipv4Cache.fullBytesCache.clear();
		}				
		
		int accCount = 0;
		for (int startByteNum = 1; startByteNum < numBytes; startByteNum++) {
			accCount += loadCacheIpv4FirstNByte (ipv4Cache, flushCacheFlag, startByteNum, numBytes, topNumRows);
		}

		if (numBytes == 4) {
			accCount += loadCacheIpv4FirstFourBytes (ipv4Cache, flushCacheFlag, topNumRows);			
		}
		
		endTime = Calendar.getInstance().getTimeInMillis();    	
		logger.debug("ipv4 loaded dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipv4Cache.numNodes +  
				" - MaxNodeLength:" + ipv4Cache.maxNodeLength + " - ipvipCache.NodeLength:" + ipv4Cache.minNodeLength +
				" - MaxNodeIpStart: " + ipv4Cache.maxNodeIpStart + " - ipv4IpCache.maxNodeIpEnd: " + ipv4Cache.maxNodeIpEnd +
				" - MinNodeIpStart: " + ipv4Cache.minNodeIpStart + " - ipv4IpCache.minNodeIpEnd: " + ipv4Cache.minNodeIpEnd);
    	
		return ipv4Cache.numNodes;
	}
	
	// @SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpv4FirstNByte (Ipv4Cache ipCache, boolean flushCacheFlag, int startByteNum, int numBytes, long topNumRows)
	  throws Throwable {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		int count = 0, midByteNum, endByteLen, locationId;
		String bytePrefixStr, byteStartStr, byteEndStr,  sql, sqlPrefix;
		long bytePrefixNum, prevBytePrefixNum = -1;
		long bytestartNum, byteEndNum;
		List<Long> startList = null, endList = null;
		List<Integer> locationIdList = null;
		Ipv4RangeNode nodeLists = null;
		
		try {			
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			midByteNum = startByteNum + 1;
			endByteLen = numBytes - startByteNum;
			if (topNumRows > 0) {
				sqlPrefix = "SELECT TOP " + topNumRows; 
			} else {
				sqlPrefix = "SELECT "; 				
			}
			
			sql = sqlPrefix + " SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") as bytePrefixStr, " + 
					"SUBSTRING(t.ipv6_start, " + midByteNum + ", " + endByteLen + ") as byteStartStr, " +
					"SUBSTRING(t.ipv6_end, " + midByteNum + ", " + endByteLen + ") as byteEndStr, * " +
					"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
					"WHERE len(t.ipv6_end) <= " + numBytes + " " +
					"and SUBSTRING(t.ipv6_start, 1, " + midByteNum + ") != SUBSTRING(t.ipv6_end, 1, " + midByteNum + ")  " +
					"and SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") = SUBSTRING(t.ipv6_end, 1, " + startByteNum + ")  " +
					"order by bytePrefixStr, byteStartStr;";
					sourceStmt = sourceConn.prepareStatement(sql);				
					
/*					
"SELECT TOP 100 SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
*/			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("");
			logger.debug("ipv4 byte(s):" + startByteNum + " get MsSqlData (ms): " + (endTime - startTime));
			startList =  new ArrayList<Long>();
			endList = new ArrayList<Long>();
			locationIdList = new ArrayList<Integer>();
			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				ipCache.numNodes++;				
				if ((count % LOAD_LOG_INTERVAL) == 1) 
				{
					logger.debug ("  Running check ipv4 byte(s)" + startByteNum + " - totalNodes:" + ipCache.numNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);				

				if (bytePrefixNum != prevBytePrefixNum) {
					
					if (nodeLists != null){
						nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
						nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
						nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());
						
						if (startList.size() > ipCache.maxNodeLength) {
							ipCache.maxNodeLength = startList.size();
							ipCache.maxNodeIpStart = sourceRs.getString("ipv6_start");
							ipCache.maxNodeIpEnd = sourceRs.getString("ipv6_end");

						}
						if (startList.size() < ipCache.minNodeLength) {
							ipCache.minNodeLength = startList.size();
							ipCache.minNodeIpStart = sourceRs.getString("ipv6_start");
							ipCache.minNodeIpEnd = sourceRs.getString("ipv6_end");
						}																				
					}
										
					nodeLists = ipCache.subBytesCache.get(bytePrefixNum);  // double check for bug. should be null
					if (nodeLists == null) {						
						nodeLists = new Ipv4RangeNode();
						startList.clear(); // startList =  new ArrayList<Long>();
						endList.clear(); // = new ArrayList<Long>();
						locationIdList.clear(); // = new ArrayList<Integer>();
						ipCache.subBytesCache.put(bytePrefixNum, nodeLists);
					} else  { // // if (prevNodeLists != nodeLists)
						logger.error("Unexpected new nodeLists != null" + bytePrefixNum + " - ipv6_start" + sourceRs.getString("ipv6_start"));
					}
				}
				
				startList.add(bytestartNum);
				endList.add(byteEndNum);
				locationIdList.add(locationId);
				prevBytePrefixNum = bytePrefixNum;
				
			}  
			if (nodeLists != null) {
				nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
				nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
				nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());				
			}
			
			if (startList.size() > ipCache.maxNodeLength) {
				ipCache.maxNodeLength = startList.size();
				ipCache.maxNodeIpStart = "startList.size() > ipCache.maxNodeLength";
				ipCache.maxNodeIpEnd = "startList.size() > ipCache.maxNodeLength";
			}
			if (startList.size() < ipCache.minNodeLength) {
				ipCache.minNodeLength = startList.size();
				ipCache.maxNodeIpStart = "startList.size() < ipCache.minNodeLength";
				ipCache.maxNodeIpEnd = "startList.size() < ipCache.minNodeLength";
			}																				
			endTime = Calendar.getInstance().getTimeInMillis();

			logger.debug("ipv4 byte(s):" + startByteNum + " - dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipCache.numNodes + " - # rows loaded:" + count  + 
				" - maxNodeLength:" + ipCache.maxNodeLength + " - minNodeLength:" + ipCache.minNodeLength +
				" - maxNodeIpStart: " + ipCache.maxNodeIpStart + " - maxNodeIpEnd: " + ipCache.maxNodeIpEnd +
				" - minNodeIpStart: " + ipCache.minNodeIpStart + " - minNodeIpEnd: " + ipCache.minNodeIpEnd);

		} catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
		} finally {
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return count;
    }

	public int loadCacheIpv6FirstNByte (Ipv6Cache ipCache, boolean flushCacheFlag, int startByteNum, int numBytes, long topNumRows) {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		int count = 0, midByteNum, endByteLen, locationId;
		String bytePrefixStr, byteStartStr, byteEndStr,  sql, sqlPrefix, ipv6_start, ipv6_end;
		BigInteger bytePrefixNum, prevBytePrefixNum = BigInteger.valueOf(-1);
		BigInteger bytestartNum, byteEndNum;
		List<BigInteger> startList = null, endList = null;
		List<Integer> locationIdList = null;
		Ipv6RangeNode nodeLists = null;
		
		try {			
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			midByteNum = startByteNum + 1;
			endByteLen = numBytes - startByteNum;
			if (topNumRows > 0) {
				sqlPrefix = "SELECT TOP " + topNumRows; 
			} else {
				sqlPrefix = "SELECT "; 				
			}
			
			sql = sqlPrefix + " SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") as bytePrefixStr, " + 
					"SUBSTRING(t.ipv6_start, " + midByteNum + ", " + endByteLen + ") as byteStartStr, " +
					"SUBSTRING(t.ipv6_end, " + midByteNum + ", " + endByteLen + ") as byteEndStr, * " +
					"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
					"WHERE len(t.ipv6_end) > " + 4 + " " +
					"and SUBSTRING(t.ipv6_start, 1, " + midByteNum + ") != SUBSTRING(t.ipv6_end, 1, " + midByteNum + ")  " +
					"and SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") = SUBSTRING(t.ipv6_end, 1, " + startByteNum + ")  " +
					"order by bytePrefixStr, byteStartStr;";
					sourceStmt = sourceConn.prepareStatement(sql);								
					
/*					
"SELECT TOP 100 SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
*/			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("");
			logger.debug("ipv6 byte(s):" + startByteNum + " get MsSqlData (ms): " + (endTime - startTime));
			startList =  new ArrayList<BigInteger>();
			endList = new ArrayList<BigInteger>();
			locationIdList = new ArrayList<Integer>();
			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");
				ipv6_start = sourceRs.getString("ipv6_start");
				ipv6_end = sourceRs.getString("ipv6_end");
						
				count++;
				ipCache.numNodes++;				
				if ((count % LOAD_LOG_INTERVAL) == 1) 
				{
					logger.debug ("  Running check ipv6 byte(s)" + startByteNum + " - totalNodes:" + ipCache.numNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				bytePrefixNum = new BigInteger (bytePrefixStr, 16);
				bytestartNum = new BigInteger (byteStartStr, 16);
				byteEndNum = new BigInteger (byteEndStr, 16);				

				if (bytePrefixNum.compareTo(prevBytePrefixNum) != 0) {
					
					if (nodeLists != null){
						nodeLists.setStartArray(convertToArray (startList));
						nodeLists.setEndArray(convertToArray (endList));
						nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());
						
						if (startList.size() > ipCache.maxNodeLength) {
							ipCache.maxNodeLength = startList.size();
							ipCache.maxNodeIpStart = ipv6_start;
							ipCache.maxNodeIpEnd = ipv6_end;

						}
						if (startList.size() < ipCache.minNodeLength) {
							ipCache.minNodeLength = startList.size();
							ipCache.minNodeIpStart = ipv6_start;
							ipCache.minNodeIpEnd = ipv6_end;
						}																				
					}
										
					nodeLists = ipCache.subBytesCache.get(bytePrefixNum);  // double check for bug. should be null
					if (nodeLists == null) {						
						nodeLists = new Ipv6RangeNode();
						startList.clear(); // startList =  new ArrayList<Long>();
						endList.clear(); // = new ArrayList<Long>();
						locationIdList.clear(); // = new ArrayList<Integer>();
						ipCache.subBytesCache.put(bytePrefixNum, nodeLists);
					} else  { // // if (prevNodeLists != nodeLists)
						logger.error("Unexpected new nodeLists != null" + bytePrefixNum + " - ipv6_start" + sourceRs.getString("ipv6_start"));
					}
				}
				
				startList.add(bytestartNum);
				endList.add(byteEndNum);
				locationIdList.add(locationId);
				prevBytePrefixNum = bytePrefixNum;				
			}  
			if (nodeLists != null) {
				nodeLists.setStartArray(convertToArray (startList));
				nodeLists.setEndArray(convertToArray (endList));
				nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());				
			}
			
			if (startList.size() > ipCache.maxNodeLength) {
				ipCache.maxNodeLength = startList.size();
				ipCache.maxNodeIpStart = "startList.size() > ipCache.maxNodeLength";
				ipCache.maxNodeIpEnd = "startList.size() > ipCache.maxNodeLength";
			}
			if (startList.size() < ipCache.minNodeLength) {
				ipCache.minNodeLength = startList.size();
				ipCache.maxNodeIpStart = "startList.size() < ipCache.minNodeLength";
				ipCache.maxNodeIpEnd = "startList.size() < ipCache.minNodeLength";
			}																				
			endTime = Calendar.getInstance().getTimeInMillis();

			logger.debug("ipv6 byte(s):" + startByteNum + " - dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipCache.numNodes + " - # rows loaded:" + count  + 
				" - maxNodeLength:" + ipCache.maxNodeLength + " - minNodeLength:" + ipCache.minNodeLength +
				" - maxNodeIpStart: " + ipCache.maxNodeIpStart + " - maxNodeIpEnd: " + ipCache.maxNodeIpEnd +
				" - minNodeIpStart: " + ipCache.minNodeIpStart + " - minNodeIpEnd: " + ipCache.minNodeIpEnd);

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return count;
    }
	
	public Map<Integer, GeoInfo> loadGeoInfo () {
		Map<Integer, GeoInfo> geoInfoMap = null;
		GeoInfo geoInfo;
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		int count = 0;
		
		try {			
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); 
			sourceStmt = sourceConn.prepareStatement(sqlGetLocationInfo);								
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("");
			logger.debug("loadGeoInfo get MsSqlData (ms): " + (endTime - startTime));
			
			startTime = Calendar.getInstance().getTimeInMillis();			
			geoInfoMap = new HashMap<Integer, GeoInfo>(GEO_INFO_CACHE_SIZE);
			while (sourceRs.next()) {
				geoInfo = new GeoInfo ();
				geoInfo.setLocationId(sourceRs.getInt("location_id"));
				geoInfo.setCountry(sourceRs.getString("country"));
				geoInfo.setRegion(sourceRs.getString("region"));
				geoInfo.setCity(sourceRs.getString("city"));
				geoInfo.setLatitude(sourceRs.getFloat("latitude"));
				geoInfo.setLongitude(sourceRs.getFloat("longitude"));
				geoInfo.setMetroCode(sourceRs.getString("metro_code"));
				geoInfoMap.put(geoInfo.getLocationId(), geoInfo);
				count++;
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadGeoInfo dur(ms):" + (endTime - startTime) + " - # locations loaded:" + count); 
		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return geoInfoMap;
    }

	private void closeDBResources (Connection sourceConn, PreparedStatement sourceStmt, ResultSet sourceRs) {
		try {
			if (sourceRs != null) {
				sourceRs.close();
			}				
		} catch (Throwable exc) {
			logger.error("", exc);
		}
		
		try {
			if (sourceStmt != null) {
				sourceStmt.close();
			}				
		} catch (Throwable exc) {
			logger.error("", exc);
		}

		try {
			if (sourceConn != null) {
				sourceConn.close();
			}				
		} catch (Throwable exc) {
			logger.error("", exc);
		}		
	}
		
	private BigInteger [] convertToArray (List<BigInteger> bigIntList) {
		if (bigIntList == null || bigIntList.size() == 0) {
			return null;
		}
		
		BigInteger bigIntArray [] = new BigInteger[bigIntList.size()];
		for (int i = 0; i < bigIntList.size(); i++) {
			bigIntArray[i] = bigIntList.get(i);
		}
		return bigIntArray;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpv4FirstFourBytes (Ipv4Cache ipCache, boolean flushCacheFlag, long topNumRows) {
		long startTime, endTime;
		PreparedStatement sourceStmt = null;
		Connection sourceConn = null;
		ResultSet sourceRs = null;
		int count = 0;
		String  byteStartStr, byteEndStr, sqlPrefix;
		int locationId;
		long bytestartNum, byteEndNum;

		try {
			if (topNumRows > 0) {
				sqlPrefix = "SELECT TOP " + topNumRows; 
			} else {
				sqlPrefix = "SELECT "; 				
			}
			
			startTime = Calendar.getInstance().getTimeInMillis();

			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(sqlPrefix + " t.ipv6_start,  t.ipv6_end, location_id " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) = 4  " +
						"and t.ipv6_start = t.ipv6_end " +
						"order by ipv6_start;" );

			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("ipv4 byte(s):4" + " get MsSqlData (ms): " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();
			while (sourceRs.next()) {
				locationId = sourceRs.getInt("location_id");
				byteStartStr = sourceRs.getString("ipv6_start");
				bytestartNum = Long.parseLong(byteStartStr, 16);				
				byteEndStr = sourceRs.getString("ipv6_end");
				byteEndNum = Long.parseLong(byteEndStr, 16);

				count++;
				ipCache.numNodes++;
				if ((count % LOAD_LOG_INTERVAL) == 1) 
				{
					logger.debug ("  Running check ipv4 byte(s)4" + " - totalNodes:" + ipCache.numNodes + " - rows:" + count +
							" - ipv6_start: " + byteStartStr + 
							" location_id:" + sourceRs.getInt("location_id"));							
				}

				if (bytestartNum != byteEndNum) {
					logger.error("bytestartNum != byteEndNum for start: " + byteStartStr + " - end:" + byteEndStr);
					return count;
				}
				
				ipCache.fullBytesCache.put(bytestartNum, locationId);			
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("ipv4 byte(s):4" + " - dur(ms):" + (endTime - startTime) + 
					" -totalNodes:" + ipCache.numNodes + " - # rows loaded:" + count  + 
					" - ipCache.maxNodeLength:" + ipCache.maxNodeLength + " - ipCache.minNodeLength:" + ipCache.minNodeLength +
					" - ipCache.maxNodeIpStart: " + ipCache.maxNodeIpStart + " - ipCache.maxNodeIpEnd: " + ipCache.maxNodeIpEnd +
					" - ipCache.minNodeIpStart: " + ipCache.minNodeIpStart + " - ipCache.minNodeIpEnd: " + ipCache.minNodeIpEnd);
		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
			this.closeDBResources(sourceConn, sourceStmt, sourceRs);
		}
		return count;
    }
	
	public int getLocationId (String ipAddress) {
		
		if (ipAddress == null) {
			return 0;
		}
		if (ipAddress.length() <= 8) {
			return this.getLocationIdIpv4(ipAddress);
		} else {
			return this.getLocationIdIpv6(ipAddress);
		}			
	}
	
	public int getLocationIdIpv4 (String ipAddress) {
		int locationId = 0;
		String bytePrefixStr, byteStr;
		long bytePrefixNum, byteNum;
		int endIndex, ipLength;
		Ipv4RangeNode nodeLists;		
		
		try {
			bytePrefixNum = Long.parseLong(ipAddress, 16);
			if (ipv4Cache.fullBytesCache.containsKey(bytePrefixNum)) {
			  return ipv4Cache.fullBytesCache.get(bytePrefixNum);
			}
			
			endIndex = 6;
			ipLength = ipAddress.length();
			while (true) {
				bytePrefixStr = ipAddress.substring(0, endIndex);				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				byteStr = ipAddress.substring(bytePrefixStr.length(), ipLength);
				byteNum = Long.parseLong(byteStr, 16); // 10448351135499550720
				nodeLists = ipv4Cache.subBytesCache.get(bytePrefixNum);
				if (null != nodeLists) {
					locationId = getLocationIdBinarySearchIpv4 (ipAddress, byteNum, nodeLists);
					if (locationId > 0) {
						break;						
					}
				} 
				endIndex -= 2;
				if (endIndex < 2) {
					break;
				}
			}			
			return locationId;
		} catch (Throwable exc) {
			logger.error("getLocationId err for ipAddress:" + ipAddress, exc);
			throw exc;
		}
	}

	public int getLocationIdIpv6 (String ipAddress) {
		int locationId = 0;
		String bytePrefixStr, byteStr;
		BigInteger bytePrefixNum, byteNum;
		int endIndex, ipLength;
		Ipv6RangeNode nodeLists;		
		
		try {
			endIndex = 16;
			ipLength = ipAddress.length();
			while (true) {
				bytePrefixStr = ipAddress.substring(0, endIndex);				
				bytePrefixNum = new BigInteger (bytePrefixStr, 16);
				byteStr = ipAddress.substring(bytePrefixStr.length(), ipLength);
				byteNum = new BigInteger (byteStr, 16); // 10448351135499550720
				nodeLists = ipv6Cache.subBytesCache.get(bytePrefixNum);
				if (null != nodeLists) {
					locationId = getLocationIdBinarySearchIpV6 (ipAddress, byteNum, nodeLists);
					if (locationId > 0) {
						break;						
					}
				} 
				endIndex -= 2;
				if (endIndex < 2) {
					break;
				}
			}			
			return locationId;
		} catch (Throwable exc) {
			logger.error("getLocationId err for ipAddress:" + ipAddress, exc);
			throw exc;
		}
	}		
	
	public int getLocationIdBinarySearchIpv4 (String ipAddress, long byteNum, Ipv4RangeNode nodeLists) {
		long [] startArray, endArray;
		int [] locationIdArray;
		int foundIndex, insertionPoint;
		
		try {
			
			startArray = nodeLists.getStartArray();
			endArray = nodeLists.getEndArray();
			locationIdArray = nodeLists.getLocationIdArray();

			foundIndex = java.util.Arrays.binarySearch (startArray, byteNum);
			if (foundIndex >= 0 && foundIndex < startArray.length) {
				// exact match
				if (byteNum <= endArray[foundIndex]) {
					// logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + foundIndex);
					return locationIdArray[foundIndex];
				}
			} else {
				// non exact match
				insertionPoint = ~foundIndex - 1;
				if (insertionPoint < 0) {
					// logger.debug("Not found ipAddress " + ipAddress);
					return 0;
				}
				if (byteNum <= endArray[insertionPoint]) {
					// logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + insertionPoint);
					return locationIdArray[insertionPoint];
				}
				
				// logger.debug("ipAddress " + ipAddress + "- byteNum " +  byteNum + " - insertionPoint " + insertionPoint);
			}			
			// logger.debug("Not found ipAddress " + ipAddress);
			return 0;
		} catch (Throwable exc) {
			exc.printStackTrace();
			throw exc;
		}
	}	

	public int getLocationIdBinarySearchIpV6 (String ipAddress, BigInteger byteNum, Ipv6RangeNode nodeLists) {
		BigInteger [] startArray, endArray;
		int [] locationIdArray;
		int foundIndex, insertionPoint;
		
		try {
			
			startArray = nodeLists.getStartArray();
			endArray = nodeLists.getEndArray();
			locationIdArray = nodeLists.getLocationIdArray();
			
			foundIndex = java.util.Arrays.binarySearch (startArray, byteNum);
			if (foundIndex >= 0 && foundIndex < startArray.length) {
				// exact match
				if (byteNum.compareTo(endArray[foundIndex]) <= 0) {
					// logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + foundIndex);
					return locationIdArray[foundIndex];
				}
			} else {
				// non exact match
				insertionPoint = ~foundIndex - 1;
				if (insertionPoint < 0) {
					// logger.debug("Not found ipAddress " + ipAddress);
					return 0;
				}
				if (byteNum.compareTo(endArray[insertionPoint]) <= 0) {
					// logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + insertionPoint);
					return locationIdArray[insertionPoint];
				}
				
				// logger.debug("ipAddress " + ipAddress + "- byteNum " +  byteNum + " - insertionPoint " + insertionPoint);
			}			
			// logger.debug("Not found ipAddress " + ipAddress);
			return 0;
		} catch (Throwable exc) {
			exc.printStackTrace();
			throw exc;
		}
	}	
	
	
	public long getIpv4NumNodes () {
		return ipv4Cache.subBytesCache.size() + ipv4Cache.fullBytesCache.size();
	}
	
	public long getIpv6NumNodes () {
		return ipv6Cache.subBytesCache.size();
	}

	/*
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpvFirstNByte_Orig (boolean flushCacheFlag, int startByteNum, long topNumRows) {
		long startTime, endTime;
		PreparedStatement sourceStmt;
		Connection sourceConn;
		ResultSet sourceRs;
		int count = 0, midByteNum, endByteLen, locationId;
		String bytePrefixStr, byteStartStr, byteEndStr,  sql, sqlPrefix;
		long bytePrefixNum, prevBytePrefixNum = -1;
		long bytestartNum, byteEndNum;
		List<Long> startList = null, endList = null;
		List<Integer> locationIdList = null;
		IpRangeNode nodeLists = null;
		
		try {			
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			midByteNum = startByteNum + 1;
			endByteLen = 4 - startByteNum;
			if (topNumRows > 0) {
				sqlPrefix = "SELECT TOP " + topNumRows; 
			} else {
				sqlPrefix = "SELECT "; 				
			}
sql = sqlPrefix + " SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") as bytePrefixStr, " + 
		"SUBSTRING(t.ipv6_start, " + midByteNum + ", " + endByteLen + ") as byteStartStr, " +
		"SUBSTRING(t.ipv6_end, " + midByteNum + ", " + endByteLen + ") as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) <= 4  " +
		"and SUBSTRING(t.ipv6_start, 1, " + midByteNum + ") != SUBSTRING(t.ipv6_end, 1, " + midByteNum + ")  " +
		"and SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") = SUBSTRING(t.ipv6_end, 1, " + startByteNum + ")  " +
		"order by bytePrefixStr, byteStartStr;";
		sourceStmt = sourceConn.prepareStatement(sql);
					
				
"SELECT TOP 100 SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("");
			logger.debug("ipv4 byte(s):" + startByteNum + " get MsSqlData (ms): " + (endTime - startTime));
			startList =  new ArrayList<Long>();
			endList = new ArrayList<Long>();
			locationIdList = new ArrayList<Integer>();
			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				ipv4NumNodes++;				
				if ((count % LOAD_LOG_INTERVAL) == 1) 
				{
					logger.debug ("  Running check ipv4 byte(s)" + startByteNum + " - totalNodes:" + ipv4NumNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);				

				if (bytePrefixNum != prevBytePrefixNum) {
					
					if (nodeLists != null){
						nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
						nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
						nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());
						
						if (startList.size() > ipv4MaxNodeLength) {
							ipv4MaxNodeLength = startList.size();
							ipv4MaxNodeIpStart = sourceRs.getString("ipv6_start");
							ipv4MaxNodeIpEnd = sourceRs.getString("ipv6_end");

						}
						if (startList.size() < ipv4MinNodeLength) {
							ipv4MinNodeLength = startList.size();
							ipv4MinNodeIpStart = sourceRs.getString("ipv6_start");
							ipv4MinNodeIpEnd = sourceRs.getString("ipv6_end");
						}																				
					}
										
					nodeLists = ipv4FirstLevelCache.get(bytePrefixNum);  // double check for bug. should be null
					if (nodeLists == null) {						
						nodeLists = new IpRangeNode();
						startList.clear(); // startList =  new ArrayList<Long>();
						endList.clear(); // = new ArrayList<Long>();
						locationIdList.clear(); // = new ArrayList<Integer>();
						ipv4FirstLevelCache.put(bytePrefixNum, nodeLists);
					} else  { // // if (prevNodeLists != nodeLists)
						logger.error("Unexpected new nodeLists != null" + bytePrefixNum + " - ipv6_start" + sourceRs.getString("ipv6_start"));
					}
				}
				
				startList.add(bytestartNum);
				endList.add(byteEndNum);
				locationIdList.add(locationId);
				prevBytePrefixNum = bytePrefixNum;
				
			}  
			if (nodeLists != null) {
				nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
				nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
				nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());				
			}
			
			if (startList.size() > ipv4MaxNodeLength) {
				ipv4MaxNodeLength = startList.size();
				ipv4MaxNodeIpStart = sourceRs.getString("ipv6_start");
				ipv4MaxNodeIpEnd = sourceRs.getString("ipv6_end");
			}
			if (startList.size() < ipv4MinNodeLength) {
				ipv4MinNodeLength = startList.size();
				ipv4MinNodeIpStart = sourceRs.getString("ipv6_start");
				ipv4MinNodeIpEnd = sourceRs.getString("ipv6_end");
			}																				
			endTime = Calendar.getInstance().getTimeInMillis();

			logger.debug("ipv4 byte(s):" + startByteNum + " - dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipv4NumNodes + " - # rows loaded:" + count  + 
				" - ipv4MaxNodeLength:" + ipv4MaxNodeLength + " - ipv4MinNodeLength:" + ipv4MinNodeLength +
				" - ipv4MaxNodeIpStart: " + ipv4MaxNodeIpStart + " - ipv4MaxNodeIpEnd: " + ipv4MaxNodeIpEnd +
				" - ipv4MinNodeIpStart: " + ipv4MinNodeIpStart + " - ipv4MinNodeIpEnd: " + ipv4MinNodeIpEnd);

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
    */
	
	/*					
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpv4FirstNByte (boolean flushCacheFlag, int startByteNum, long topNumRows) {
		long startTime, endTime;
		PreparedStatement sourceStmt;
		Connection sourceConn;
		ResultSet sourceRs;
		int count = 0, midByteNum, endByteLen, locationId;
		String bytePrefixStr, byteStartStr, byteEndStr,  sql, sqlPrefix;
		long bytePrefixNum, prevBytePrefixNum = -1;
		long bytestartNum, byteEndNum;
		List<Long> startList = null, endList = null;
		List<Integer> locationIdList = null;
		IpRangeNode nodeLists = null;
		
		try {			
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			midByteNum = startByteNum + 1;
			endByteLen = 4 - startByteNum;
			if (topNumRows > 0) {
				sqlPrefix = "SELECT TOP " + topNumRows; 
			} else {
				sqlPrefix = "SELECT "; 				
			}
sql = sqlPrefix + " SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") as bytePrefixStr, " + 
		"SUBSTRING(t.ipv6_start, " + midByteNum + ", " + endByteLen + ") as byteStartStr, " +
		"SUBSTRING(t.ipv6_end, " + midByteNum + ", " + endByteLen + ") as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) <= 4  " +
		"and SUBSTRING(t.ipv6_start, 1, " + midByteNum + ") != SUBSTRING(t.ipv6_end, 1, " + midByteNum + ")  " +
		"and SUBSTRING(t.ipv6_start, 1, " + startByteNum + ") = SUBSTRING(t.ipv6_end, 1, " + startByteNum + ")  " +
		"order by bytePrefixStr, byteStartStr;";
		sourceStmt = sourceConn.prepareStatement(sql);
					
"SELECT TOP 100 SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("");
			logger.debug("ipv4 byte(s):" + startByteNum + " get MsSqlData (ms): " + (endTime - startTime));
			startList =  new ArrayList<Long>();
			endList = new ArrayList<Long>();
			locationIdList = new ArrayList<Integer>();
			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				ipv4NumNodes++;				
				if ((count % LOAD_LOG_INTERVAL) == 1) 
				{
					logger.debug ("  Running check ipv4 byte(s)" + startByteNum + " - totalNodes:" + ipv4NumNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);				

				if (bytePrefixNum != prevBytePrefixNum) {
					
					if (nodeLists != null){
						nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
						nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
						nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());
						
						if (startList.size() > ipv4MaxNodeLength) {
							ipv4MaxNodeLength = startList.size();
							ipv4MaxNodeIpStart = sourceRs.getString("ipv6_start");
							ipv4MaxNodeIpEnd = sourceRs.getString("ipv6_end");

						}
						if (startList.size() < ipv4MinNodeLength) {
							ipv4MinNodeLength = startList.size();
							ipv4MinNodeIpStart = sourceRs.getString("ipv6_start");
							ipv4MinNodeIpEnd = sourceRs.getString("ipv6_end");
						}																				
					}
										
					nodeLists = ipv4FirstLevelCache.get(bytePrefixNum);  // double check for bug. should be null
					if (nodeLists == null) {						
						nodeLists = new IpRangeNode();
						startList.clear(); // startList =  new ArrayList<Long>();
						endList.clear(); // = new ArrayList<Long>();
						locationIdList.clear(); // = new ArrayList<Integer>();
						ipv4FirstLevelCache.put(bytePrefixNum, nodeLists);
					} else  { // // if (prevNodeLists != nodeLists)
						logger.error("Unexpected new nodeLists != null" + bytePrefixNum + " - ipv6_start" + sourceRs.getString("ipv6_start"));
					}
				}
				
				startList.add(bytestartNum);
				endList.add(byteEndNum);
				locationIdList.add(locationId);
				prevBytePrefixNum = bytePrefixNum;
				
			}  
			if (nodeLists != null) {
				nodeLists.setStartArray(startList.stream().mapToLong(i->i).toArray());
				nodeLists.setEndArray(endList.stream().mapToLong(i->i).toArray());
				nodeLists.setLocationIdArray(locationIdList.stream().mapToInt(i->i).toArray());				
			}
			
			if (startList.size() > ipv4MaxNodeLength) {
				ipv4MaxNodeLength = startList.size();
				ipv4MaxNodeIpStart = sourceRs.getString("ipv6_start");
				ipv4MaxNodeIpEnd = sourceRs.getString("ipv6_end");
			}
			if (startList.size() < ipv4MinNodeLength) {
				ipv4MinNodeLength = startList.size();
				ipv4MinNodeIpStart = sourceRs.getString("ipv6_start");
				ipv4MinNodeIpEnd = sourceRs.getString("ipv6_end");
			}																				
			endTime = Calendar.getInstance().getTimeInMillis();

			logger.debug("ipv4 byte(s):" + startByteNum + " - dur(ms):" + (endTime - startTime) + 
				" -totalNodes:" + ipv4NumNodes + " - # rows loaded:" + count  + 
				" - ipv4MaxNodeLength:" + ipv4MaxNodeLength + " - ipv4MinNodeLength:" + ipv4MinNodeLength +
				" - ipv4MaxNodeIpStart: " + ipv4MaxNodeIpStart + " - ipv4MaxNodeIpEnd: " + ipv4MaxNodeIpEnd +
				" - ipv4MinNodeIpStart: " + ipv4MinNodeIpStart + " - ipv4MinNodeIpEnd: " + ipv4MinNodeIpEnd);

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
*/			
		
	/*
	public int loadCacheIpv4FlattenedCache (boolean flushCacheFlag, int lowerImportId, int upperImportId) {
		int accCount = 0;
		long startTime, endTime;

		accCount += this.loadCacheIpv4FirstFourBytes(flushCacheFlag, 10);
		
		startTime = Calendar.getInstance().getTimeInMillis();
		accCount += this.loadCacheIpv4FirstThreeBytes(flushCacheFlag, lowerImportId, upperImportId);
		endTime = Calendar.getInstance().getTimeInMillis();
		logger.debug("loadCacheIpv4-: # rows loaded:" + accCount  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		startTime = Calendar.getInstance().getTimeInMillis();
		accCount += this.loadCacheIpv4FirstTwoBytes(flushCacheFlag, lowerImportId, upperImportId);
		endTime = Calendar.getInstance().getTimeInMillis();
		logger.debug("loadCacheIpv4-: # rows loaded:" + accCount  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		startTime = Calendar.getInstance().getTimeInMillis();
		accCount += this.loadCacheIpv4FirstOneByte_FlattenedCache(flushCacheFlag, lowerImportId, upperImportId);
		endTime = Calendar.getInstance().getTimeInMillis();
		logger.debug("loadCacheIpv4-: # rows loaded:" + accCount  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		logger.debug("loadCacheIpv4-: maxSubRange:" + ipv4MaxSubRange  + " - minSubRange:" + ipv4MinSubRange);
		return accCount;
		
	}
		
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public long loadCacheIpv4FirstThreeBytes (boolean flushCacheFlag, int lowerImportId, int upperImportId) {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0, range;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId;
		long bytestartNum, byteEndNum;
		HashMap<Long, Long> secondLevelCache;		

		try {

			startTime = Calendar.getInstance().getTimeInMillis();

			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 3) as bytePrefixStr, SUBSTRING(t.ipv6_start, 4, 1) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 4, 1) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 4) != SUBSTRING(t.ipv6_end, 1, 4)  " +
						"and SUBSTRING(t.ipv6_start, 1, 3) = SUBSTRING(t.ipv6_end, 1, 3)  " +
						"order by bytePrefixStr, byteStartStr;" );

			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");
								
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);
				secondLevelCache = ipv4FirstLevel_FlattenedCache.get(bytePrefixNum);
				if (secondLevelCache == null) {
					secondLevelCache = new HashMap<Long, Long>(range);					
					ipv4FirstLevel_FlattenedCache.put(bytePrefixNum, secondLevelCache);
				}
				
				for (long index = bytestartNum; index <= byteEndNum; index++) {
					secondLevelCache.put(index, locationId);
				}

				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				ipv4NumNodes += range;
			
				if ((count % 1000) == 0) 
				{
					logger.debug ("loadCacheIpv4FirstThreeBytes- totalNodes:" + ipv4NumNodes + " - rows:" + count + 
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				count++;
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			ipv4NumNodes += count;

			logger.debug("loadCacheIpv4FirstThreeBytes-: # rows loaded:" + count  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpv4FirstTwoBytes (boolean flushCacheFlag, int lowerImportId, int upperImportId) {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0, range;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId;;
		long bytestartNum, byteEndNum;
		HashMap<Long, Long> secondLevelCache;		

		try {

			startTime = Calendar.getInstance().getTimeInMillis();

			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 2) as bytePrefixStr, SUBSTRING(t.ipv6_start, 3, 2) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 3, 2) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 3) != SUBSTRING(t.ipv6_end, 1, 3)  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) = SUBSTRING(t.ipv6_end, 1, 2)  " +
						"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");
								
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

				secondLevelCache = ipv4FirstLevel_FlattenedCache.get(bytePrefixNum);
				if (secondLevelCache == null) {
					secondLevelCache = new HashMap<Long, Long> (range);					
					ipv4FirstLevel_FlattenedCache.put(bytePrefixNum, secondLevelCache);
				}
				
				for (long index = bytestartNum; index <= byteEndNum; index++) {
					secondLevelCache.put(index, locationId);
				}
				
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				ipv4NumNodes += range;

				if ((count % 1000) == 0) 
				{
					logger.debug ("loadCacheIpv4FirstTwoBytes- totalNodes:" + ipv4NumNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				count++;
			}
			endTime = Calendar.getInstance().getTimeInMillis();
			ipv4NumNodes += count;

			logger.debug("loadCacheIpv4FirstTwoBytes-: # rows loaded:" + count  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int loadCacheIpv4FirstOneByte_FlattenedCache (boolean flushCacheFlag, int lowerImportId, int upperImportId) {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0, range;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId;
		long bytestartNum, byteEndNum;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1 = null, secondLevelCacheTmp2 = null;		

		try {

			startTime = Calendar.getInstance().getTimeInMillis();

			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT TOP 23 SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				if ((count % 10) == 1) 
				{
					logger.debug ("loadCacheIpv4FirstOneByte- totalNodes:" + ipv4NumNodes + " - rows:" + count +
							" location_id:" + sourceRs.getInt("location_id") +							
							" - bytePrefixStr: " + bytePrefixStr +
							" - byteStartStr: " + byteStartStr +
							" - byteEndStr: " + byteEndStr +							
							" - ipv6_start: " + sourceRs.getString("ipv6_start") +
							" - ipv6_end: " + sourceRs.getString("ipv6_end")); 
				}
				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

				secondLevelCache = ipv4FirstLevel_FlattenedCache.get(bytePrefixNum);
				if (secondLevelCache == null) {
					secondLevelCache = new HashMap<Long, Long> (range);					
					ipv4FirstLevel_FlattenedCache.put(bytePrefixNum, secondLevelCache);
				} else {
					// logger.debug("secondLevelCache != null for " + bytePrefixNum);
				}
								
				for (long index = bytestartNum; index <= byteEndNum; index++) {
					if (secondLevelCache.containsKey(index)) {
						logger.error("secondLevelCache  != null for bytePrefixNum " + bytePrefixNum + " - entry " + index);						
					}
					secondLevelCache.put(index, locationId);
				}
				
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				ipv4NumNodes += range;
				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			ipv4NumNodes += count;

			logger.debug("loadCacheIpv4FirstOneByte-: # rows loaded:" + count  + " - totalNodes:" + ipv4NumNodes + " - dur(ms):" + (endTime - startTime));

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
	*/
	
	/*
	public int getIpv4NumNodes_FlattenedCache () {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0, range;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId;
		long bytestartNum, byteEndNum;
		long numNodes;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1 = null, secondLevelCacheTmp2 = null;		

		try {

			numNodes = 0;
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 1) as bytePrefixStr, SUBSTRING(t.ipv6_start, 2, 3) as byteStartStr, " +
						"SUBSTRING(t.ipv6_end, 2, 3) as byteEndStr, * " +
						"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
						"WHERE len(t.ipv6_end) <= 4  " +
						"and SUBSTRING(t.ipv6_start, 1, 2) != SUBSTRING(t.ipv6_end, 1, 2)  " +
						"and SUBSTRING(t.ipv6_start, 1, 1) = SUBSTRING(t.ipv6_end, 1, 1)  " +
						"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

								
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				numNodes += range;
				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			numNodes += count;
			ipv4NumNodes += numNodes; 
			logger.debug ("loadCacheIpv4FirstOneByte- numNodes:" + numNodes + " - rows:" + count + " - dur(ms):" + (endTime - startTime));
						
			numNodes = 0;
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 2) as bytePrefixStr, SUBSTRING(t.ipv6_start, 3, 2) as byteStartStr, " +
		"SUBSTRING(t.ipv6_end, 3, 2) as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) <= 4  " +
		"and SUBSTRING(t.ipv6_start, 1, 3) != SUBSTRING(t.ipv6_end, 1, 3)  " +
		"and SUBSTRING(t.ipv6_start, 1, 2) = SUBSTRING(t.ipv6_end, 1, 2)  " +
		"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

								
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				numNodes += range;
				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			numNodes += count;
			ipv4NumNodes += numNodes; 
			logger.debug ("loadCacheIpv4FirstTwoBytes- numNodes:" + numNodes + " - rows:" + count + " - dur(ms):" + (endTime - startTime));
			
			numNodes = 0;
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 3) as bytePrefixStr, SUBSTRING(t.ipv6_start, 4, 1) as byteStartStr, " +
		"SUBSTRING(t.ipv6_end, 4, 1) as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) <= 4  " +
		"and SUBSTRING(t.ipv6_start, 1, 4) != SUBSTRING(t.ipv6_end, 1, 4)  " +
		"and SUBSTRING(t.ipv6_start, 1, 3) = SUBSTRING(t.ipv6_end, 1, 3)  " +
		"order by bytePrefixStr, byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				bytePrefixStr = sourceRs.getString("bytePrefixStr");
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");
				locationId = sourceRs.getInt("location_id");

				count++;
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

								
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				numNodes += range;
				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			numNodes += count;
			ipv4NumNodes += numNodes; 
			logger.debug ("loadCacheIpv4FirstThreeBytes- numNodes:" + numNodes + " - rows:" + count + " - dur(ms):" + (endTime - startTime));
			logger.debug ("loadCacheIpv4First to ThreeBytes- totalNodes:" + ipv4NumNodes);
			

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
	*/

	/*
	public int getIpv4NumNodesAll () {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		int count = 0;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId, range;
		long bytestartNum, byteEndNum;
		long numNodes;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1 = null, secondLevelCacheTmp2 = null;		
		
		try {			
			numNodes = 0;
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT t.ipv6_start as byteStartStr, " +
		"t.ipv6_end as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) <= 4  " +
		"order by byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");

				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);
								
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				numNodes += range;
				count++;
				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			numNodes += count;
			ipv4NumNodes += numNodes; 
			logger.debug ("loadCacheIpv4All- numNodes:" + numNodes + " - rows:" + count + 
				" - maxSubRange:" + ipv4MaxSubRange + " - minSubRange:" + ipv4MinSubRange + " - dur(ms):" + (endTime - startTime));
			logger.debug ("loadCacheIpv4All- totalNodes:" + ipv4NumNodes);
			

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
	*/
	/*
	public long getIpv6NumNodesAll () {
		long startTime, endTime;
		Map<String, Object> paramMap = null;
		PreparedStatement sourceStmt, insertStmt;
		Connection sourceConn, destConn;
		ResultSet sourceRs;
		long count = 0;
		String bytePrefixStr, byteStartStr, byteEndStr, ipStart, ipEnd;
		long bytePrefixNum, locationId, range;
		long bytestartNum, byteEndNum;
		long numNodes;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1 = null, secondLevelCacheTmp2 = null;		

		try {			
			numNodes = 0;
			startTime = Calendar.getInstance().getTimeInMillis();
			Class.forName("net.sourceforge.jtds.jdbc.Driver");
			sourceConn = dataSource.getConnection(); //  DriverManager.getConnection(url, username, password);
			sourceStmt = sourceConn.prepareStatement(
"SELECT SUBSTRING(t.ipv6_start, 1, 8) as byteStartStr, " +
		"SUBSTRING(t.ipv6_end, 1, 8)as byteEndStr, * " +
		"FROM _shared.dbo.ipv6_city_A t  (nolock) " +
		"WHERE len(t.ipv6_end) > 4  " +
		"order by byteStartStr;" );
			
			sourceRs = sourceStmt.executeQuery();
			endTime = Calendar.getInstance().getTimeInMillis();
			logger.debug("loadIpv6Lookup: get MsSqlData: " + (endTime - startTime));

			startTime = Calendar.getInstance().getTimeInMillis();			
			while (sourceRs.next()) {
				byteStartStr = sourceRs.getString("byteStartStr");
				byteEndStr = sourceRs.getString("byteEndStr");

				bytestartNum = Long.parseLong(byteStartStr, 16);
				byteEndNum = Long.parseLong(byteEndStr, 16);
				range = (int)(byteEndNum - bytestartNum + 1);

								
				if (range > ipv4MaxSubRange) {
					ipv4MaxSubRange = range;
				}
				if (range < ipv4MinSubRange) {
					ipv4MinSubRange = range;
				}
				numNodes += range;
				count++;				
			}  
			endTime = Calendar.getInstance().getTimeInMillis();
			numNodes += count;
			ipv4NumNodes += numNodes; 
			logger.debug ("loadCacheIpv6All- numNodes:" + numNodes + " - rows:" + count + 
					" - maxSubRange:" + ipv4MaxSubRange + " - minSubRange:" + ipv4MinSubRange + " - dur(ms):" + (endTime - startTime));
			logger.debug ("loadCacheIpvAll- totalNodes:" + ipv4NumNodes);
			

		} catch (Throwable exc) {
			logger.error("", exc);
		} finally {
		}
		return count;
    }
	*/	
	
	/*
	public static void main (String args []) {
		
		BigInteger a = new BigInteger ("10"), b = new BigInteger ("20"), c = new BigInteger ("9100000000000001", 16), d;
		BigInteger bigDecArray [] = {a, b, c};
		List<BigInteger> bdList = new ArrayList<BigInteger> ();
		
		d = new BigInteger ("9100000000000000", 16);
		int insertionPoint,  foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		d = new BigInteger ("15");
		foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		d = new BigInteger ("20");
		foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		d = new BigInteger ("25");
		foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		d = new BigInteger ("30");
		foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		d = new BigInteger ("35");
		foundIndex = java.util.Arrays.binarySearch (bigDecArray, d);
		if (foundIndex >= 0) {
			logger.debug("found " + d);
		} else {
			// non exact match
			insertionPoint = ~foundIndex - 1;			
			if (insertionPoint < 0) {
				logger.debug("Not found  " + d);
			} else {
				logger.debug("insertionPoint:" + insertionPoint);				
			}
		}
		
		
		HashCacheDao hashCacheDao = new HashCacheDao ();
		
		hashCacheDao.loadCacheIpv6 (true, 3);
		
	}
	
	static public int getLocationIdBinarySearch_Test (String ipAddress) {
		int locationId = 0;
		String bytePrefixStr, byteStr, byteEndStr;
		int bytePrefixNum, endIndex;
		long byteNum = -1, locationIdNew;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1, secondLevelCacheTmp2;
		boolean containFlag;
		long startRangeArray []; // = {0x060000, 0x0C0000, 0x100000};
		long endRangeArray [] = {0x07FFFF, 0x0FFFFF, 0x1274FF};
		int foundIndex, insertionPoint;
		List<Integer> startRangeList = new ArrayList<Integer> ();
		int [] a;
		
		try {
			
			startRangeList.add(0x060000);
			startRangeList.add(0x0C0000);
			startRangeList.add(0x100000);
			startRangeArray = startRangeList.stream().mapToLong(i->i).toArray();
			
			byteStr = ipAddress.substring(2, 8);
			byteNum = Long.parseLong(byteStr, 16);
			// byteNum = 0x0C00A0;
			foundIndex = java.util.Arrays.binarySearch (startRangeArray, byteNum);
			if (foundIndex >= 0 && foundIndex < startRangeArray.length) {
				if (byteNum <= endRangeArray[foundIndex]) {
					logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + foundIndex);
					return foundIndex;
				}
			} else {
				insertionPoint = ~foundIndex - 1;
				if (insertionPoint < 0) {
					logger.debug("Not found ipAddress " + ipAddress);
					return 0;
				}
				if (byteNum <= endRangeArray[insertionPoint]) {
					logger.debug("found ipAddress " + ipAddress + "- byteNum " +  byteNum + " foundIndex:" + insertionPoint);
					return insertionPoint;
				}
				
				// logger.debug("ipAddress " + ipAddress + "- byteNum " +  byteNum + " - insertionPoint " + insertionPoint);
			}
			
			logger.debug("Not found ipAddress " + ipAddress);
			return 0;
		} catch (Throwable exc) {
			exc.printStackTrace();
			throw exc;
		}
	}
	*/

	/*
	public long getLocationId_FlattenedCache (String ipAddress) {
		long locationId = 0;
		String bytePrefixStr, byteStr, byteEndStr;
		long bytePrefixNum;
		int endIndex;
		long byteNum, locationIdNew;
		HashMap<Long, Long> secondLevelCache, secondLevelCacheTmp1, secondLevelCacheTmp2;
		boolean containFlag;
		
		try {
			bytePrefixNum = Long.parseLong(ipAddress, 16);
			if (ipCache.fullBytesCache.containsKey(bytePrefixNum)) {
			  return ipCache.fullBytesCache.get(bytePrefixNum);
			}			
			
			endIndex = 6;
			while (true) {
				bytePrefixStr = ipAddress.substring(0, endIndex);
				
				bytePrefixNum = Long.parseLong(bytePrefixStr, 16);
				byteStr = ipAddress.substring(bytePrefixStr.length(), 8);
				byteNum = Long.parseLong(byteStr, 16);
				secondLevelCacheTmp1 = ipv4FirstLevel_FlattenedCache.get(bytePrefixNum); //060000 01
				if (null != secondLevelCacheTmp1 && secondLevelCacheTmp1.containsKey(byteNum)) {
					locationId = secondLevelCacheTmp1.get(byteNum);
					break;
				} else {
					endIndex -= 2;
					if (endIndex < 2) {
						break;
					}
				}	 			
			}			
			return locationId;
		} catch (Throwable exc) {
			exc.printStackTrace();
			throw exc;
		}
	}
	*/
	
	/*
	private HashMap<String, String> testfirstLevelCache;
	
	public int loadDummyData (boolean flushCacheFlag, int maxNumOps) {

		int numOps = 0;
		byte b1 = 15, b2 = 127, b3 = -127;
		StringBuffer key;

		try {

			System.out.println ("hnp.getHost():" + hnp.getHost() + " - maxNumOps:" + maxNumOps);
			if (flushCacheFlag && testfirstLevelCache != null && testfirstLevelCache.size() > 0) {
				testfirstLevelCache.clear();	
			}
			testfirstLevelCache = new HashMap<String, String>(maxNumOps);
			
			Object value;
			key = new  StringBuffer ("foo");
			long begin = Calendar.getInstance().getTimeInMillis();
	
			for (numOps = 0; numOps <= maxNumOps; numOps++) {
				testfirstLevelCache.put(key.toString() + numOps, "bar" + numOps);
			}
			long elapsed = Calendar.getInstance().getTimeInMillis() - begin;
			System.out.println("Set maxNumOps: " + maxNumOps + " elapsed(ms): " + elapsed + " - in sec:" + (float)((float)elapsed / 1000.0));
			float opsrate = (float)maxNumOps / (float)elapsed;
			System.out.println("Set ops/ms = " + opsrate + " - ops/sec: " + opsrate * 1000);
	
			begin = Calendar.getInstance().getTimeInMillis();
			for (numOps = 0; numOps <= maxNumOps; numOps++) {
				value = testfirstLevelCache.get(key.toString() + numOps);
				if (value == null) {
					System.out.println("key:" + key.toString() + numOps + " - value is null");					
				}
			}
			elapsed = Calendar.getInstance().getTimeInMillis() - begin;
			System.out.println("Get maxNumOps: " + maxNumOps + " elapsed(ms): " + elapsed + " - in sec:" + (float)((float)elapsed / 1000.0));
			opsrate = (float)maxNumOps / (float)elapsed;
			System.out.println("Get ops/ms = " + opsrate + " - ops/sec: " + opsrate * 1000);

		} catch (Throwable exc) {
			exc.printStackTrace();
		}

		return numOps;
	}
	*/
}
