/*
 * Michael M. Nguyen
 */
package com.getcake.geo.nginxhandler;


import static nginx.clojure.MiniConstants.*;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.log4j.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.controller.GeoController;
import com.getcake.geo.model.LoadStatistics;
import com.getcake.geo.service.IpInvalidException;
import com.getcake.geo.service.IpNotFoundException;
import com.getcake.util.CakeCommonUtil;

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class GeoIPLookupHandler implements NginxJavaRingHandler {
		
	private static int invokeCount = 0, constructorCount = 0;
	private Object [] retObjs;
		
	private static final Logger logger = Logger.getLogger(GeoIPLookupHandler.class);
	
	static GeoController geoController;
	
	public GeoIPLookupHandler () {
		
		try {				
				if (geoController != null) {
					logger.debug("GeoIPLookupApp - GeoIPLookupApp.geoIPLookupApp != null");
					return;
				}
				
				String current = new java.io.File( "." ).getCanonicalPath();
		        String currentDir = System.getProperty("user.dir");		        
		        Path currentRelativePath = Paths.get("");
		        String absCurrPath = Paths.get("").toAbsolutePath().toString();
			        
				++constructorCount;
				retObjs = new Object [3];
				retObjs[0] = NGX_HTTP_OK;
				retObjs[1] = ArrayMap.create(CONTENT_TYPE, "text/plain");
				retObjs[2] = invokeCount;
				
	    	    geoController = GeoController.getInstance();
	    	    geoController.init("geoservices.properties");
				
	    		geoController.loadHashCacheDao(false, -1);
	    		geoController.selfTest();
			} catch (Throwable exc) {
				exc.printStackTrace();
				logger.error("", exc);				
			}
		}

	    public static void main(String[] args) {
	    	
	    	try {
		    	LoadStatistics loadStatistics = new LoadStatistics();
		    	loadStatistics.setAvgAlgorthmDurationMicroSec (2);
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
            	query_string = (String)request.get("query-string");
        		strTokenizer = new StringTokenizer (query_string, "=");
        		ipAddressList = strTokenizer.nextToken();
        		ipAddressList = strTokenizer.nextToken();        		
            	geoInfo = geoController.getMultiGeoInfo(ipAddressList);
                return new Object[] { 
                        NGX_HTTP_OK, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"),
                        geoInfo
                        };
        	} catch (IpNotFoundException exc) {
                return new Object[] { 
                		NGX_HTTP_NOT_FOUND, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        CakeCommonUtil.convertExceptionToString (exc) };        		
        	} catch (IpInvalidException exc) {
                return new Object[] { 
                		NGX_HTTP_BAD_REQUEST, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        CakeCommonUtil.convertExceptionToString (exc) };        		
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        CakeCommonUtil.convertExceptionToString (exc) };        		
        	}        	
        }
    }