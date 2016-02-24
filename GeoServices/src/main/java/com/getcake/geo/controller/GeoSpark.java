package com.getcake.geo.controller;

import static spark.Spark.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;




import javax.servlet.http.HttpServletResponse;




import org.apache.log4j.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.getcake.geo.service.IpInvalidException;
import com.getcake.geo.service.IpNotFoundException;
import com.getcake.geo.service.JsonTransformer;



import com.getcake.util.CakeCommonUtil;

import spark.Spark;

// import org.apache.hive.jdbc.HiveDriver;

public class GeoSpark {

	private static final Logger logger = Logger.getLogger(GeoSpark.class);
	private static GeoController geoController;
	private static final String VERSION = "0.05";
	
    private static AWSCredentialsProvider credentialsProvider;

    private static void init() {
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\mnguyen\\.aws\\credentials).
         */
        credentialsProvider = new ProfileCredentialsProvider(); 
        try {
            credentialsProvider.getCredentials();
            System.out.println (credentialsProvider.getCredentials().getAWSAccessKeyId());
            System.out.println (credentialsProvider.getCredentials().getAWSSecretKey());
            
        } catch (Exception exc) {
        	logger.error("Cannot load the credentials from the credential profiles file. ", exc);
        	throw exc;
        }
    }

	
    public static void main(String[] args) {

    	JsonTransformer jsonTransformer;
    	// ApplicationContext applicationContext;
    	int minthreads, maxthreads, idleTimeoutMillis, portNum = 8080; 
    	String dataSourceType = null, appPropFile, dbServer;
    	Properties properties;
    	
    	try {
    		// init ();
    		
    		Map<String, String> env = System.getenv();
    		/*
            for (String envName : env.keySet()) {
                System.out.format("%s=%s%n",
                                  envName,
                                  env.get(envName));
            } */
            
        	// String test = convertToString ("FFFF");
    		// org.eclipse.jetty.util.thread.QueuedThreadPool a;
    		
        	if (args.length < 5 ) {
        		System.out.println ("usage: <minthreads> <maxthreads> <idleTimeoutMillis> <dataSourceType: msSql or sparkSql> <dbServerName> <optional: Spark Port #>");
        		return;
        	} 
        	
        	/*
        	dataSourceType = args[3];        		
        	if ("mssql".equalsIgnoreCase(dataSourceType)) {
        		appPropFile = "mssql.geoservices.properties";
        	} else if ("sparksql".equalsIgnoreCase(dataSourceType)) {
        		appPropFile = "sparksql.geoservices.properties";
        	} else {
        		appPropFile = "sparksql.geoservices.properties";
        	}
        	dbServer = args[4];
        	*/
        	
        	/*
    		applicationContext = new ClassPathXmlApplicationContext(appContextFile);    		        		        	
    		geoController = applicationContext.getBean("geoController", GeoController.class); //  new GeoController (); //
    		*/    			
        	
	        // String absCurrPath = Paths.get("").toAbsolutePath().toString();
	        // logger.debug("Current toAbsolutePath path is: " + absCurrPath);
    	    geoController = GeoController.getInstance();
    	    properties = geoController.init("geoservices.properties");
    	    
        	System.out.println ("GeoSpark " + VERSION + " - dbServer: " + properties.getProperty("dbServer") + 
        			" - appPropFile: " + "geoservices.properties");
        	logger.debug ("GeoSpark " + VERSION + " - dbServer: " + properties.getProperty("dbServer") + 
        			" - appPropFile: " + "geoservices.properties");

        	/* logger.debug("loadHashCacheDao start");
    		geoController.loadHashCacheDao(false, -1);
        	logger.debug("loadHashCacheDao done"); */
        	minthreads = Integer.parseInt(properties.getProperty("minthreads")); // Integer.parseInt(args[0]);
        	maxthreads = Integer.parseInt(properties.getProperty("maxthreads")); //Integer.parseInt(args[1]);
        	idleTimeoutMillis = Integer.parseInt(properties.getProperty("idleTimeoutMillis")); //Integer.parseInt(args[2]);

        	// staticFileLocation("/public");
        	// externalStaticFileLocation("c:/download");
        	
        	/*if (args.length > 5) */ {
        		portNum = Integer.parseInt(properties.getProperty("sparkPort")); // Integer.parseInt(args[5]);
        	}
        	Spark.port(portNum);
        	Spark.threadPool(maxthreads, minthreads, idleTimeoutMillis);
        	logger.debug("GeoSpark " + VERSION + " - portNum: " + portNum + " - minthreads:" + minthreads + " - maxthreads:" + maxthreads + " - idleTimeoutMillis:" + idleTimeoutMillis);
        	System.out.println("GeoSpark v1 minthreads:" + minthreads + " - maxthreads:" + maxthreads + " - idleTimeoutMillis:" + idleTimeoutMillis);
        	            
    		jsonTransformer = new JsonTransformer ();
        	
            get("/geoservices/test", (request, response) -> {
                return "test ok";
            });

            get("/geoservices/health", (request, response) -> {
                try {
					return geoController.healthCheck();
            	} catch (IpNotFoundException exc) {
            		response.status(HttpServletResponse.SC_NOT_FOUND);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	} catch (IpInvalidException exc) {
            		response.status(HttpServletResponse.SC_BAD_REQUEST);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	}
            });

            get("/geoservices/statuscount", (request, response) -> {
                return geoController.getStatusCount();
            });

            get("/geoservices/statistics", (request, response) -> {
                return geoController.getGeoInfoStatistics();
            }, jsonTransformer);
    		
            put("/geoservices/statistics", (request, response) -> {
                return geoController.resetGeoInfoStatistics();
            }, jsonTransformer);
    		
            get("/geoservices/ipv4numnodes", (request, response) -> {
                return geoController.getIpv4NumNodes();
            }, jsonTransformer);
    		
            get("/geoservices/ipv6numnodes", (request, response) -> {
                return geoController.getIpv6NumNodes();
            }, jsonTransformer);
    		
            get ("/geoservices/load", (request, response)  -> {
            	boolean flushCacheFlag;
            	long topNumRows;
            	
            	try {
            		flushCacheFlag = Boolean.getBoolean(request.queryParams("flushcacheflag"));
            	} catch (Throwable exc) {
            		flushCacheFlag = true;
            	}
            	 
            	try {
            		topNumRows = Long.parseLong(request.queryParams("topnumrows"));
            	} catch (Throwable exc) {
            		topNumRows = -1;
            	}
            	 
            	try {
                    return geoController.loadHashCacheDao(flushCacheFlag, topNumRows);            		
            	} catch (Throwable exc) {
            		return CakeCommonUtil.convertExceptionToString (exc);
            	}
            }, jsonTransformer);
    		
            get("/geoservices/geo", (request, response) -> {  
            	try {
                    return geoController.getMultiGeoInfo(request.queryParams("ipaddresses"));            		
            	} catch (IpNotFoundException exc) {
            		response.status(HttpServletResponse.SC_NOT_FOUND);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	} catch (IpInvalidException exc) {
            		response.status(HttpServletResponse.SC_BAD_REQUEST);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	} catch (Throwable exc) {
            		logger.error(exc);
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		return CakeCommonUtil.convertExceptionToString (exc);
            	}
            }); 
    		
    	} catch (Throwable exc) {
    		logger.error("", exc);
    	}
    }

	public static byte[] hexStringToByteArray(String inputStr) {
		byte[] data; 
		int len;
		try {
		    len = inputStr.length();
		    data = new byte[len / 2];
			// System.out.println ("str:" + inputStr + " len:" + len);
		    for (int i = 0; i < len; i += 2) {
				// System.out.println ("i:" + i);		    		
		    	if (i >= 32) {
					System.out.println ("err i:" + i);		    		
		    	}
		        data[i / 2] = (byte) ((Character.digit(inputStr.charAt(i), 16) << 4)
		                             + Character.digit(inputStr.charAt(i+1), 16));
		    }			
		    return data;
		} catch (Throwable exc) {
			System.out.println ("err str:" + inputStr);
			exc.printStackTrace();
			throw exc;
		}
	}		

	private static String convertToString (String ipAddress) {
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
        	System.out.println ("ipAddrStr: " + ipAddrStr);
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
    	} catch (Throwable exc) {
    		logger.info("Invalid input IP: " + ipAddress);
    		return null;
    	}
		return strBuf.toString();
	}
	
    
}
