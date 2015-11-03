package com.getcake.geo.nginxhandler;

/**
 * Hello world!
 *
 */

import static nginx.clojure.MiniConstants.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.StringTokenizer;


// import javax.servlet.http.HttpServletResponse;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.controller.GeoController;
import com.getcake.geo.model.LoadStatistics;
import com.getcake.geo.service.IpInvalidException;
import com.getcake.geo.service.IpNotFoundException;

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class GeoIPLookupHandler implements NginxJavaRingHandler {
		
	private static int invokeCount = 0, constructorCount = 0;
	private Object [] retObjs;
		
	private static final Logger logger = Logger.getLogger(GeoIPLookupHandler.class);
	
	// private ApplicationContext applicationContext;
	static GeoController geoController;
	
	public GeoIPLookupHandler () {
		
		try {				
				/*
	    		Map<String, String> env = System.getenv();
	            for (String envName : env.keySet()) {
	                logger.debug(envName + ": " + env.get(envName));
	            }
				*/
				
				if (geoController != null) {
					logger.debug("GeoIPLookupApp - GeoIPLookupApp.geoIPLookupApp != null");
					return;
				}
				
				String current = new java.io.File( "." ).getCanonicalPath();
				logger.debug("Current dir:"+current);
		        String currentDir = System.getProperty("user.dir");
		        logger.debug("Current dir using System:" +currentDir);
		        
		        Path currentRelativePath = Paths.get("");
		        logger.debug("Current relative path is: " + currentRelativePath.toString());
		        String absCurrPath = Paths.get("").toAbsolutePath().toString();
		        logger.debug("Current toAbsolutePath path is: " + absCurrPath);
			        
				++constructorCount;
				System.out.println ("App constructor constructorCount: " + constructorCount);
				logger.debug("App constructor constructorCount: " + constructorCount);
				retObjs = new Object [3];
				retObjs[0] = NGX_HTTP_OK;
				retObjs[1] = ArrayMap.create(CONTENT_TYPE, "text/plain");
				retObjs[2] = invokeCount;
				
	    	    geoController = GeoController.getInstance();
	    	    geoController.init("geoservices.properties");
				
	        	logger.debug("loadHashCacheDao start");
	    		geoController.loadHashCacheDao(false, -1);
	    		geoController.getStatus();
	        	logger.debug("loadHashCacheDao done");
	    	    
			} catch (Throwable exc) {
				exc.printStackTrace();
				logger.error("", exc);				
			}
		}

	    public static void main(String[] args) {
	    	
	    	try {
		    	LoadStatistics loadStatistics = new LoadStatistics();
		    	// loadStatistics.accDuration = 1;
		    	loadStatistics.avgAlgDurationMicroSec = 2;
		    	loadStatistics.count = 3;
	    		ObjectMapper mapper = new ObjectMapper();
	    		String statStr = mapper.writeValueAsString(loadStatistics);
	    		System.out.println (statStr);
	    	} catch (Throwable exc) {
	    		exc.printStackTrace();
	    	}	    	
		}
		
        // @Override
        public Object[] invoke(Map<String, Object> request) {
        	String query_string, ipAddressList;
    		StringTokenizer strTokenizer;
        	String geoInfo;
        	
        	try {
    			// System.out.println ("Public App invoke constructorCount: " + constructorCount);
    			// logger.debug("App invoke constructorCount: " + constructorCount);
    			
            	query_string = (String)request.get("query-string");
        		strTokenizer = new StringTokenizer (query_string, "=");
        		ipAddressList = strTokenizer.nextToken();
        		ipAddressList = strTokenizer.nextToken();
            	logger.debug("ipAddressList: " + ipAddressList);
        		
        		/*
            	Iterator keys = request.keySet().iterator();
            	while (keys.hasNext()) {
            		Object key = keys.next();
                	logger.debug("key: " + key + " - value: " + request.get(key));            		
            	}
            	*/
        		
            	geoInfo = geoController.getMultiGeoInfo(ipAddressList);
            	// logger.debug("ipAddress: " + ipAddress + " loc id: " + geoInfo.getLocationId());
                return new Object[] { 
                        NGX_HTTP_OK, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        geoInfo
                        };
        	} catch (IpNotFoundException exc) {
                return new Object[] { 
                		NGX_HTTP_NOT_FOUND, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        exc.getLocalizedMessage() };        		
        	} catch (IpInvalidException exc) {
                return new Object[] { 
                		NGX_HTTP_BAD_REQUEST, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        exc.getLocalizedMessage() };        		
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        "Unexpected error of " + exc.getLocalizedMessage()
                        };        		
        	}
        	
        }
    }