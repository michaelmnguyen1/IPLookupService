package com.getcake.geo.service;

import java.math.BigInteger;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.getcake.geo.controller.GeoController;
import com.getcake.geo.dao.*;
import com.getcake.geo.model.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GeoService {

	protected static final Logger logger = LoggerFactory.getLogger(GeoService.class);
	
	/* memSqlDao, elasticCacheDao and redisCacheDao are no longer used.  They are left here for historical purpsoes */
	/*
	@Autowired
	private MsSqlDao msSqlDao;
	
	@Autowired
	private MemSqlDao memSqlDao;
	
	@Autowired
	private ElasticCacheDao elasticCacheDao;
	
	@Autowired
	private RedisCacheDao redisCacheDao;
	*/
	
	@Autowired
	private HashCacheDao hashCacheDao;
	
	private Map<Integer, GeoInfo> geoInfoMap;
	
	public long loadHashCacheDao (boolean flushCacheFlag, long topNumRows) throws Throwable  {
		geoInfoMap = hashCacheDao.loadGeoInfo ();
		return hashCacheDao.loadCache(flushCacheFlag, topNumRows);
	}	
	
	public long getIpv4NumNodes () {
		return hashCacheDao.getIpv4NumNodes();
	}	
	
	public long getIpv6NumNodes () {
		return hashCacheDao.getIpv6NumNodes();
	}	
		
	public long getLocationId (String ipAddress) {
		return hashCacheDao.getLocationId(ipAddress);
	}	
	
	public GeoInfo getGeoInfo (String ipAddress) {
    	GeoInfo geoInfo;
    	int locationId;
    	locationId = hashCacheDao.getLocationId(ipAddress);
    	if (locationId >= 0) {
    		return geoInfoMap.get(locationId);
    	}
    	return null;    			
	}
	
	/*
	 * 
	public GeoInfo getGeoInfoDirect (BigInteger ipv4, String ipv6) {
    	GeoInfo geoInfo;
    	int locationId;
    	
    	locationId = memSqlDao.findLocationIdDirect(ipv6);
    	// logger.debug("memSql getGeoInfoDirect locationId: " + locationId);
    	geoInfo = new GeoInfo ();
    	geoInfo.setLocationId(locationId);
    	geoInfo.setCountry("US");
    	
    	return geoInfo;
	}
	
	public GeoInfo getGeoInfoByMsSql (BigInteger ipv4, String ipv6) {
    	GeoInfo geoInfo;
    	int locationId;
    	
    	locationId = msSqlDao.findLocationId(null, ipv6);
    	// logger.debug("Microsoft Sql locationId: " + locationId);
    	
    	geoInfo = new GeoInfo ();
    	geoInfo.setLocationId(locationId);
    	geoInfo.setCountry("US");
    	
    	return geoInfo;
	}
	
	public int loadDummyDataHashCacheDao (boolean flushCacheFlag, int numOps) {
		return hashCacheDao.loadDummyData(flushCacheFlag, numOps);
	}	


	public int insertGeoLookup (int lowerImportId, int upperImportId) {
		return msSqlDao.transferIpv6Lookup(memSqlDao.getDataSource(), lowerImportId, upperImportId);
	}
		
	public int loadEleasticache (boolean flushCacheFlag) {
		return elasticCacheDao.loadCache(flushCacheFlag, 0, 0);
	}	
	
	public int loadLocalRedisCache (boolean flushCacheFlag) {
		return redisCacheDao.loadCache(flushCacheFlag, 0, 0);
	}	
	
	 * 
	 */
}
