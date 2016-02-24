package com.getcake.geo.service;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.dao.*;
import com.getcake.geo.dao.HashCacheDao;
import com.getcake.geo.dao.GeoMsSqlDao;
import com.getcake.geo.dao.S3Dao;
import com.getcake.geo.model.*;
import com.getcake.util.NullValueSerializer;

import org.apache.log4j.*;

public class GeoService {

	protected static final Logger logger = Logger.getLogger(GeoService.class);
	
	private static GeoService instance;
	
	// private MemSqlDao memSqlDao;
	private GeoMsSqlDao msSqlDao;
    private S3Dao s3Dao;    
	private HashCacheDao hashCacheDao;
	
	private Map<Integer, String> geoInfoMap;
	private Map<Integer, String> ispInfoMap;
	private Properties properties;

    private String ipv6_cityFileName, citiesFileName, ipv6_ispFileName, ispsFileName;    
	private String topLevelDataPath, region;	
	static final String EXPORT_FILE_SUFFIX = ".csv";
	
	private String blankIspInfoJsonFragment;
	
	static {
		try {
			instance = new GeoService ();
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}
	
	static public GeoService getInstance () {
		return instance;
	}
	
	public GeoService () throws JsonProcessingException {
		ObjectMapper jsonMapper = new ObjectMapper();
		jsonMapper.getSerializerProvider().setNullValueSerializer(new NullValueSerializer());
		
		GeoISP geoIsp = new GeoISP();
		geoIsp.ispInfo = new IspInfo(); // "stub isp info";
		
		StringBuilder jsonOutput = new StringBuilder();
		jsonOutput.append(jsonMapper.writeValueAsString(geoIsp));
		jsonOutput.delete(0, jsonOutput.indexOf("isp_info", 0) - 2);
		
		blankIspInfoJsonFragment = jsonOutput.toString();
	}
	
	private static final long MILLISEC_PER_MINUTE = 1000 * 60;
	public long loadHashCacheDao (boolean flushCacheFlag, long topNumRows) throws Throwable  {
		long ipNodeCount = -1, ispNodeCount;
		long startTime, ispStartTime, locationStartTime, endTime, durationMillisec;
				
		logger.debug("=================");
		logger.debug("ISP loading start");
		startTime = Calendar.getInstance().getTimeInMillis();
		ispStartTime = startTime;
		ispInfoMap = hashCacheDao.loadIspInfo_Binary ();
		ispNodeCount = hashCacheDao.loadIspCache(flushCacheFlag, topNumRows);
		endTime = Calendar.getInstance().getTimeInMillis();
		durationMillisec = endTime - ispStartTime;
		logger.debug("ISP Total ipv4 and ipv6 nodes are " + ispNodeCount + 
			" - dur(ms):" + durationMillisec + " - dur(min):" + (float)((float)durationMillisec / (float)MILLISEC_PER_MINUTE));
		logger.debug("");
		
		logger.debug("=================");
		logger.debug("Location loading start");
		locationStartTime = Calendar.getInstance().getTimeInMillis();
		geoInfoMap = hashCacheDao.loadGeoInfo_Binary ();
		ipNodeCount = hashCacheDao.loadLocationCache(flushCacheFlag, topNumRows);
		endTime = Calendar.getInstance().getTimeInMillis();
		durationMillisec = endTime - locationStartTime;
		logger.debug("Location Total ipv4 and ipv6 nodes are " + ipNodeCount  + 
			" - dur(ms):" + durationMillisec + " - dur(min):" + (float)((float)durationMillisec / (float)MILLISEC_PER_MINUTE));

		durationMillisec = endTime - ispStartTime;
		logger.debug("ISP and Location Total Load Time - dur(ms):" + durationMillisec + 
			" - dur(min):" + (float)((float)durationMillisec / (float)MILLISEC_PER_MINUTE));
		return ipNodeCount + ispNodeCount;		
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
	
	public String getGeoInfo (String ipAddress, String origIpAddress) throws IpNotFoundException {
    	StringBuilder geoInfo;
    	int locationId, isp_id;
    	
    	locationId = hashCacheDao.getLocationId(ipAddress);
    	if (locationId == 0) {
    		throw new IpNotFoundException ("IP address not found for " + ipAddress);
    	}
    	geoInfo = new StringBuilder ();
    	geoInfo.append("{\"ip_address\": \"");
    	geoInfo.append(origIpAddress);
    	geoInfo.append("\",");
    	
    	geoInfo.append("\"http_status_code\":");
    	geoInfo.append("\"200\"");
    	
    	geoInfo.append(geoInfoMap.get(locationId));
    			
    	isp_id = hashCacheDao.getIspId(ipAddress);
    	if (isp_id == 0) {
    		geoInfo.append(blankIspInfoJsonFragment);    		
    	} else {
        	geoInfo.append(ispInfoMap.get(isp_id));    		
    	}
    	
    	return geoInfo.toString();    	
	}

	public void init (Properties properties) {
		this.properties = properties;
		this.hashCacheDao = HashCacheDao.getInstance();
		hashCacheDao.init(properties);
	}

	public void iniForExportImport (Properties properties) {
	    topLevelDataPath = properties.getProperty("topLevelDataPath");
	    region = properties.getProperty("region"); 
	    if (region != null) {
	    	topLevelDataPath += "-" + region;	    	
	    } 
	    
		this.msSqlDao = GeoMsSqlDao.getInstance();
		msSqlDao.init(properties);		
		this.s3Dao = S3Dao.getInstance();
	}

    public String getGeoDataVersion () {
    	return hashCacheDao.getGeoDataVersion ();
    }
	
    public MsSqlExportCheckResp exportMsSqlGeoData () throws Throwable {
    	MsSqlExportCheckResp msSqlExportCheckResp;
    	Date latestIpImportDate;
    	String bucketName;
    	String [] outputs;
        StringBuilder respMsg = new StringBuilder ();
    	
        msSqlExportCheckResp = new MsSqlExportCheckResp ();
        latestIpImportDate = msSqlDao.getLatestIpImportDate();
        if (latestIpImportDate == null) {
        	return null;
        }
        
	    bucketName = topLevelDataPath.substring(topLevelDataPath.indexOf("//") + 2, topLevelDataPath.length());
	    outputs = s3Dao.getNewDataVersion(bucketName, properties.getProperty("geoTopLevelFolder"), latestIpImportDate);
	    msSqlExportCheckResp.prevExportedIpVersion = outputs[0];
	    if (outputs[1] == null) {
	    	respMsg.append("latestIpImportDate of " + latestIpImportDate + " is the same or older than latest exported data of " + outputs[0] +
	    		". Skip export");
	    	logger.debug(respMsg.toString());
	    	msSqlExportCheckResp.detailMsg = respMsg.toString();
	    } else {
	    	logger.debug("latestIpImportDate of " + latestIpImportDate + " is newer than latest exported data of " + outputs[0] +
		    		". Calling export");
	    	logger.debug(exportMsSqlGeoData (outputs[1]));
	    	msSqlExportCheckResp.newIpVersion = outputs[1];
	    	msSqlExportCheckResp.detailMsg = respMsg.toString();
	    }
    	return msSqlExportCheckResp;    	
    }
	    
    public String exportMsSqlGeoData (String exportGeoDataVer) throws Throwable {
        String  bucketName, bucketFolder, outputFileName;
        StringBuilder respMsg = new StringBuilder ();
        S3Dao s3Dao;
        String outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName;
        
	    bucketName = topLevelDataPath.substring(topLevelDataPath.indexOf("//") + 2, topLevelDataPath.length());
	    citiesFileName = properties.getProperty("citiesFileName");
	    ipv6_cityFileName = properties.getProperty("ipv6_cityFileName");
	    ispsFileName = properties.getProperty("ispsFileName");    	
	    ipv6_ispFileName = properties.getProperty("ipv6_ispFileName");
	    
    	outputDirName = this.properties.getProperty("geoExporTopLevelOutputDir") + properties.getProperty("region") + 
    		"/ipdata/" + exportGeoDataVer + "/";
		geoFileName = outputDirName + citiesFileName + exportGeoDataVer + EXPORT_FILE_SUFFIX;
		ipGeoRangeFileName = outputDirName + ipv6_cityFileName + exportGeoDataVer + EXPORT_FILE_SUFFIX;
		ispFilename = outputDirName + ispsFileName + exportGeoDataVer + EXPORT_FILE_SUFFIX;
		ipIspRangeFileName = outputDirName + ipv6_ispFileName + exportGeoDataVer + EXPORT_FILE_SUFFIX;		
	    
	    respMsg.append(exportMsSqlGeoData (outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName));

    	s3Dao = new S3Dao ();
	    bucketFolder = properties.getProperty("geoTopLevelFolder") + "/" + exportGeoDataVer;
	    outputFileName = citiesFileName + exportGeoDataVer + ".csv";    
    	s3Dao.writeS3TextInputStream(geoFileName, bucketName, bucketFolder, outputFileName);
    	
	    bucketFolder = properties.getProperty("geoTopLevelFolder") + "/" + exportGeoDataVer;
	    outputFileName = ipv6_cityFileName + exportGeoDataVer + ".csv";    
    	s3Dao.writeS3TextInputStream(ipGeoRangeFileName, bucketName, bucketFolder, outputFileName);

	    bucketFolder = properties.getProperty("geoTopLevelFolder") + "/" + exportGeoDataVer;
	    outputFileName = ispsFileName + exportGeoDataVer + ".csv";    
    	s3Dao.writeS3TextInputStream(ispFilename, bucketName, bucketFolder, outputFileName);

	    bucketFolder = properties.getProperty("geoTopLevelFolder") + "/" + exportGeoDataVer;
	    outputFileName = ipv6_ispFileName + exportGeoDataVer + ".csv";    
    	s3Dao.writeS3TextInputStream(ipIspRangeFileName, bucketName, bucketFolder, outputFileName);

    	return respMsg.toString();    	
    }
    
    public String exportMsSqlGeoData (String outputDirName, String geoFileName, String ipGeoRangeFileName, String ispFilename,
        	String ipIspRangeFileName) throws Throwable {
    	return msSqlDao.exportMsSqlGeoData (outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName);
    }
    
	/*
	static public void main (String args []) {
		GeoService geoService = new GeoService ();
		
		geoService.memSqlDao = new MemSqlDao ();
		geoService.msSqlDao = new MsSqlDao ();
		
		geoService.msSqlDao.transferIpv6Lookup(memSqlDao., lowerImportId, upperImportId)
		
	}
	*/
	
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
