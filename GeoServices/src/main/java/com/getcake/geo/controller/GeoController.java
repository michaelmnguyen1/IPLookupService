package com.getcake.geo.controller;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.model.*;
import com.getcake.geo.service.GeoService;
import com.getcake.geo.service.IpInvalidException;
import com.getcake.geo.service.IpNotFoundException;
import com.getcake.util.AwsUtil;
import com.getcake.util.CakeCommonUtil;
import com.getcake.util.NullValueSerializer;

public class GeoController {
	
	private static final Logger logger = Logger.getLogger(GeoController.class);
	private static GeoController instance;
	    
    private GeoService geoService;
    private LoadStatistics loadStatistics; 
    private Properties properties;    
    private String selfTestIpv4, selfTestIpv6, initStatus;
    private long statusCount = 0;
	private ObjectMapper jsonMapper;
    
	static {
		try {
		    // logger.debug("EC2MetadataUtils.getAvailabilityZone (): " + EC2MetadataUtils.getAvailabilityZone ());
		    // System.out.println("EC2MetadataUtils.getAvailabilityZone (): " + EC2MetadataUtils.getAvailabilityZone ());
			instance = new GeoController ();
			// logger.debug("GeoController static init instance: " + instance);
			// System.out.println("GeoController static init instance: " + instance);
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}
	
	static public GeoController getInstance () {
		// logger.debug("GeoController getInstance instance: " + instance);
		// System.out.println("GeoController getInstance instance: " + instance);
		return instance;
	}
	
    public GeoController () {
    	loadStatistics = new LoadStatistics ();
		jsonMapper = new ObjectMapper();
		// jsonMapper.getSerializerProvider().setNullValueSerializer(new NullValueSerializer());    	
    }
    
    public GeoService getGeoService () {
    	return geoService;
    }
    
    public void setGeoService (GeoService geoInfoService) {
    	this.geoService = geoInfoService;
    }
    	
	Map<Integer, GeoInfo> empData = new HashMap<Integer, GeoInfo>();

    public long loadHashCacheDao(
    	boolean flushCacheFlag, 
    	long topNumRows) throws Throwable  {
    	
    	long numNodesLoaded;
    	
    	try {
        	numNodesLoaded = geoService.loadHashCacheDao(flushCacheFlag, topNumRows);
        	initStatus = "Geo Init Succeeded";
        	return numNodesLoaded;
    	} catch (Throwable exc) {
    		initStatus = CakeCommonUtil.convertExceptionToString (exc);
    		throw exc;
    	}
    }

    public String getMultiGeoInfo(String ipAddressList) throws IpNotFoundException, IpInvalidException {
    	String ipAddresses [];
    	String ipAddress;
    	StringBuilder geoInfoList = null;
    	GeoISP geoIspResponse;
    	
    	if (ipAddressList == null || ipAddressList.trim().length() == 0) {
    		return "";
    	}
    	
    	ipAddresses = ipAddressList.split(",");
    	
    	geoInfoList = new StringBuilder ();
		// geoInfoList.append("{ \"geo_info_list\" : ");
		geoInfoList.append('[');
    	for (int i = 0; i < ipAddresses.length; i++ ) {
    		ipAddress = null;
        	try {
        		if (i > 0) {
        			geoInfoList.append(',');
        	    }
        		ipAddress = ipAddresses[i];
        		geoInfoList.append(getGeoInfo (ipAddress));
        	} catch (IpInvalidException exc) {
        		geoIspResponse = new GeoISP ();
        		geoIspResponse.ipAddress = ipAddress;
        		geoIspResponse.httpResponseCode = "400";
        		try {
					geoInfoList.append(jsonMapper.writeValueAsString(geoIspResponse));
				} catch (JsonProcessingException exc2) {
					logger.error(geoIspResponse, exc2);
				}        		
        	} catch (IpNotFoundException exc) {
        		geoIspResponse = new GeoISP ();
        		geoIspResponse.ipAddress = ipAddress;
        		geoIspResponse.httpResponseCode = "404";        		
        		try {
					geoInfoList.append(jsonMapper.writeValueAsString(geoIspResponse));
				} catch (JsonProcessingException exc2) {
					logger.error(geoIspResponse, exc2);
				}        		
        	}
    	}
		geoInfoList.append("]");    	
    	return geoInfoList.toString();    	    	
    }
    
    public String getGeoInfo(String origIpAddress) throws IpNotFoundException, IpInvalidException {
    	String geoInfo, ipAddress;
    	long algorthmStartTime, allProcessingStartTime, endTime, ipFormatConvDur, algorthmDur;
    	
		allProcessingStartTime = System.nanoTime();
    	if (origIpAddress == null || origIpAddress.trim().length() == 0) {
    		logger.info("input IP is blank");
    		throw new IpInvalidException ("input IP is blank");
    	}
    	
    	// logger.debug("Input ipAddress: " + ipAddress);
    	ipAddress = convertToString (origIpAddress);
    	// logger.debug("Output ipAddress: " + ipAddress);
    	
		algorthmStartTime = System.nanoTime();
    	ipFormatConvDur = algorthmStartTime - allProcessingStartTime;
    	geoInfo = geoService.getGeoInfo(ipAddress, origIpAddress);
    	endTime = System.nanoTime();
    	algorthmDur = endTime - algorthmStartTime;
    	/* logger.debug("convertToString dur(microseconds): " + (convDur / 1000));
    	duration = endTime - startTime;
    	logger.debug("alg dur(microseconds): " + (algDur / 1000));
    	logger.debug("total dur(microseconds): " + (duration / 1000)); */
    	
    	/*
    	if (duration > loadStatistics.maxDuration) {
    		loadStatistics.maxDuration = duration;
    	}
    	if (duration < loadStatistics.minDuration) {
    		loadStatistics.minDuration = duration;
    	}
    	// do not use synchronized on purpose for increments of accDuration and count to avoid impact on performance
    	// for the statistics purposes, these calculations do not need to be exact. 
    	*/
    	loadStatistics.allIpFormatConvDurationNanoSec += ipFormatConvDur;
    	loadStatistics.allAlgorthmDurationNanoSec += algorthmDur;    	    	
    	loadStatistics.count++;
    	return geoInfo;
    }

	private String convertToString (String ipAddress) throws IpInvalidException {
		InetAddress inetAddress;
		String ipAddrStr = null, ipComp, hexStr;
		StringBuilder strBuf;
		StringTokenizer strTokenizer;
		try {
        	if (ipAddress.indexOf(':') >= 0 || ipAddress.indexOf('.') >= 0) {
            	inetAddress = InetAddress.getByName(ipAddress);    		
        	}  else if (ipAddress.trim().length() >= 8 ){
        		return ipAddress;
        	} else {
        		throw new IpInvalidException ("Invalid IP: " + ipAddress);
        	}
        	
        	strBuf = new StringBuilder ();
        	ipAddrStr = inetAddress.getHostAddress();
        	if (ipAddrStr.indexOf('.') >= 0) {
        		strTokenizer = new StringTokenizer (ipAddrStr, ".");
        		while (strTokenizer.hasMoreElements()) {
        			ipComp = strTokenizer.nextToken();
        			hexStr = Integer.toHexString(Integer.parseInt(ipComp));
        			for (int i = 2; i > hexStr.length(); i--) {
            			strBuf.append("0");        				
        			}
        			strBuf.append(hexStr);
        		}
        	} else {
        		strTokenizer = new StringTokenizer (ipAddrStr, ":");
        		while (strTokenizer.hasMoreElements()) {
        			ipComp = strTokenizer.nextToken();        		
        			for (int i = 4; i > ipComp.length(); i--) {
            			strBuf.append("0");        				
        			}
        			strBuf.append(ipComp);
        		}
        	}
    	} catch (IpInvalidException exc) {
    		throw exc;
    	} catch (Throwable exc) {
    		logger.info("Invalid input IP: " + ipAddress);
    		throw new IpInvalidException ("Invalid IP: " + ipAddress);
    	}
		return strBuf.toString();
	}
	    
    // @RequestMapping(value = "statistics", method = RequestMethod.GET) throws Throwable
    public LoadStatistics getGeoInfoStatistics()  {
    	loadStatistics.setAvgAlgorthmDurationMicroSec ((loadStatistics.allAlgorthmDurationNanoSec / (double)loadStatistics.count) / 1000);
    	loadStatistics.setAvgIpFormatConvDurationMicroSec ((loadStatistics.allIpFormatConvDurationNanoSec / (double)loadStatistics.count) / 1000);
    	
    	// ObjectMapper mapper = new ObjectMapper();
    	// return mapper.writeValueAsString (loadStatistics);
    	return loadStatistics;
    }    
    
    // @RequestMapping(value = "statistics", method = RequestMethod.PUT)
    public LoadStatistics resetGeoInfoStatistics() {
    	loadStatistics.count = 0;
    	loadStatistics.allAlgorthmDurationNanoSec = 0;
    	/*
    	loadStatistics.maxDuration = 0;
    	loadStatistics.minDuration = Long.MAX_VALUE;
    	*/
    	loadStatistics.setAvgAlgorthmDurationMicroSec (0);
    	loadStatistics.setAvgIpFormatConvDurationMicroSec (0);
    	return loadStatistics;
    }    
    
    // @RequestMapping(value = "locid", method = RequestMethod.GET) ipaddress
    public long getLocationId(String ipAddress) {
    	
		return geoService.getLocationId(ipAddress);
		// long startTime, endTime, locationId;
    	// logger.debug("/locid");
		// startTime = Calendar.getInstance().getTimeInMillis();
		// endTime = Calendar.getInstance().getTimeInMillis();
    	// logger.debug("/locid locationId: " + locationId + " dur(ms): " + (endTime - startTime));		
		// return locationId;
    }
	
    // @RequestMapping(value = "ipv4numnodes", method = RequestMethod.GET)
    public long getIpv4NumNodes() {
    	
    	logger.debug("/ipv4numnodes");
    	return geoService.getIpv4NumNodes();
    }

    public long getIpv6NumNodes() {
    	
    	logger.debug("/ipv6numnodes");
    	return geoService.getIpv6NumNodes();
    }

    public String selfTest () throws IpNotFoundException, IpInvalidException {
    	long startTime, durationMicrosec;
    	String resp1, resp2;
		StringBuilder jsonResp;
		boolean errFlag = false;
    	
		jsonResp = new StringBuilder ();
		jsonResp.append(" \"self_test_ipv4\":");
		
    	startTime = System.nanoTime();
        try {
            resp1 = this.getGeoInfo(selfTestIpv4);        	
        } catch (Throwable exc) {
        	errFlag = true;
        	resp1 = "\"" + CakeCommonUtil.convertExceptionToString (exc) + "\"" ;        		        	
        }
		logger.debug("result of ipv4 self-test " + selfTestIpv4 + ": " + resp1);	    		
		jsonResp.append(resp1);
		
		jsonResp.append(", \"self_test_ipv6\":");
        try {
        	resp2 = this.getGeoInfo(selfTestIpv6);
        } catch (Throwable exc) {
        	errFlag = true;
        	resp2 = "\"" + CakeCommonUtil.convertExceptionToString (exc) + "\"" ;        		        	
        }
		logger.debug("result of ipv6 self-test " + selfTestIpv6 + ": " + resp2);	    		
		jsonResp.append(resp2);
		
		jsonResp.append(", \"self_test_duration_micro_seconds\": \"");    	
        durationMicrosec = (System.nanoTime() - startTime) / 1000;
		jsonResp.append(durationMicrosec);    	
		jsonResp.append("\" ");    	
        
		if (!errFlag) {
			return jsonResp.toString();			
		} else {
			throw new RuntimeException (jsonResp.toString());			
		}
    	// return " \"self_test_ipv4_" + selfTestIpv4 + "\":"  + resp1 + ", \"self_test_ipv6_" + selfTestIpv6 + "\":" + resp2 +
    	//	", \"self_test_duration_micro_seconds\": \"" + durationMicrosec + "\" ";
    }

    public String healthCheck () throws IpNotFoundException, IpInvalidException, JsonProcessingException {
		String selfTestResults, statisticsJson;
		StringBuilder healtResp;
		boolean errFlag = false;
		
		healtResp = new StringBuilder ();
		healtResp.append("{");
		healtResp.append("\"init_status\":\"");
		healtResp.append(initStatus);
		healtResp.append("\",");
		
		try {
			selfTestResults = selfTest();			
		} catch (Throwable exc) {
			errFlag = true;
			selfTestResults = exc.getMessage();
			/*
			healtResp.append("\"self_test_failed_reason\": \"");
			healtResp.append(CakeCommonUtil.convertExceptionToString (exc));        					
			healtResp.append("\""); */
		} 
		
		healtResp.append(selfTestResults);		
		healtResp.append(",");
		
		healtResp.append("\"load_statistics\": ");
		try {
			statisticsJson = jsonMapper.writeValueAsString(getGeoInfoStatistics());			
			healtResp.append(statisticsJson);
		} catch (Throwable exc) {
			errFlag = true;
			healtResp.append("\"");
			healtResp.append(CakeCommonUtil.convertExceptionToString (exc));
			healtResp.append("\"");
		}		
		
		healtResp.append("}");
    				
		if (!errFlag) {
	    	return healtResp.toString();			
		} else {
			throw new RuntimeException (healtResp.toString());
		}
    }
    
    public long getStatusCount() {    	
    	return ++statusCount;
    }

    public Properties loadProperties (String propFileName) throws IOException {
		properties = new Properties ();
		properties.load(new FileInputStream (propFileName));
		return properties;
    	
		/*
    	inputStream = getClass().getResourceAsStream(propFileName);
    	if (inputStream != null) {
    		properties = new Properties ();
    		properties.load(inputStream);
    		return properties;
		} else {
			throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
		} */   	
    }
    
    public Properties init (String propFileName)  throws IOException {
	    properties = AwsUtil.loadProperties (propFileName);
	    
	    selfTestIpv4 = properties.getProperty("selfTestIpv4");
	    selfTestIpv6 = properties.getProperty("selfTestIpv6");
    	
    	this.geoService = GeoService.getInstance();
    	geoService.init(properties);
    	return properties;
    }
    
    public Properties initWithMsSqlDao (String propFileName)  throws IOException {
    	init (propFileName);
    	geoService.iniForExportImport(properties);    	
    	return properties;
    }
    
    public String getGeoDataVersion () {
    	return geoService.getGeoDataVersion();
    }

    public String exportMsSqlGeoData (String exportGeoDataVer) throws Throwable {
    	return this.geoService.exportMsSqlGeoData(exportGeoDataVer);
    }
    
    public MsSqlExportCheckResp exportMsSqlGeoData () throws Throwable {
    	return this.geoService.exportMsSqlGeoData();
    }
    
    /*
    private String ipv6_cityFileName, citiesFileName, ipv6_ispFileName, ispsFileName, sparkMaster, appName, sparkConfigFileName;    
    private String geoSourceFilePath, geoDataVersion; // "s3://cake-deployment-artifacts-us-west/ipdata/"; //  
    private String topLevelDataPath, region;
    
	static final String EXPORT_FILE_SUFFIX = ".csv";
    
    public String exportMsSqlGeoData () throws Throwable {
        String ipv6_cityPathFileName, citiesPathFileName, ipv6_ispPathFileName, ispsPathFileName, 
        	bucketName, bucketFolder, outputFileName;
        StringBuilder respMsg = new StringBuilder ();
        S3Dao s3Dao;
        String outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName;
        
    	s3Dao = new S3Dao ();

        //

    	return respMsg.toString();    	
    }
	
    public String exportMsSqlGeoData (String exportGeoDataVer) throws Throwable {
        String ipv6_cityPathFileName, citiesPathFileName, ipv6_ispPathFileName, ispsPathFileName, 
        	bucketName, bucketFolder, outputFileName;
        StringBuilder respMsg = new StringBuilder ();
        S3Dao s3Dao;
        String outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName;
        
	    topLevelDataPath = properties.getProperty("topLevelDataPath");
	    region = properties.getProperty("region"); 
	    if (region != null) {
	    	topLevelDataPath += "-" + region;	    	
	    } 
	    
	    bucketName = topLevelDataPath.substring(topLevelDataPath.indexOf("//") + 2, topLevelDataPath.length());
	    geoSourceFilePath = topLevelDataPath + "/" + properties.getProperty("geoTopLevelFolder") + "/";
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
	    
	    respMsg.append(geoService.exportMsSqlGeoData (outputDirName, geoFileName, ipGeoRangeFileName, ispFilename, ipIspRangeFileName));

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
	*/
    
    public static void main(String[] args) {
    	
    	GeoController geoController;
    	try {    		
    		geoController = GeoController.getInstance();
    		geoController.properties = geoController.init("cake-spark-jdbc-server.conf"); // geoservices.properties
    		geoController.exportMsSqlGeoData(args[0]);
    	} catch (Throwable exc) {
    		logger.error("", exc);
    	}
    }    
}
