package com.getcake.geo.nginxhandler;

/**
 * Hello world!
 *
 */

import static nginx.clojure.MiniConstants.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.controller.GeoController;
import com.getcake.geo.model.GeoInfo;
import com.getcake.geo.model.LoadStatistics;

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class GeoInfoStatisticsHandler implements NginxJavaRingHandler {

		public Object [] retObjs;
		static int invokeCount = 0, constructorCount = 0;
		
		private static final Logger logger = Logger.getLogger(GeoInfoStatisticsHandler.class);
		
    	private GeoController geoController;
		private ObjectMapper jsonMapper;
		
		public GeoInfoStatisticsHandler () {
			
			try {
				jsonMapper = new ObjectMapper();
				if (GeoIPLookupHandler.geoController == null) {
					logger.debug("GeoInfoStatisticsApp - GeoIPLookupApp.geoController == null");
				}
	        	logger.debug("GeoInfoStatisticsApp done");
	    		
			} catch (Throwable exc) {
				exc.printStackTrace();
				logger.error("", exc);				
			}
		}

	    public static void main(String[] args) {
	    	GeoInfoStatisticsHandler app = new GeoInfoStatisticsHandler ();
		}
		
        // @Override
        public Object[] invoke(Map<String, Object> request) {
        	String requestMethod;
        	
        	try {
        		if ("get".equalsIgnoreCase((String)request.get("request-method"))) {
                	// logger.debug("ipAddress: " + ipAddress + " loc id: " + geoInfo.getLocationId());
            		// loadStatistics = GeoIPLookupApp.geoController.getGeoInfoStatistics();
                    return new Object[] { 
                            NGX_HTTP_OK, //http status 200
                            ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                            jsonMapper.writeValueAsString(GeoIPLookupHandler.geoController.getGeoInfoStatistics())                        		
                            //response body can be string, File or Array/Collection of them
                            };        			
        		}
        		
            	// logger.debug("ipAddress: " + ipAddress + " loc id: " + geoInfo.getLocationId());
        		// loadStatistics = GeoIPLookupApp.geoController.getGeoInfoStatistics();
                return new Object[] { 
                        NGX_HTTP_OK, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        jsonMapper.writeValueAsString(GeoIPLookupHandler.geoController.resetGeoInfoStatistics())                        		
                        //response body can be string, File or Array/Collection of them
                        };
        		
        		
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        "Java exc: " + exc.getMessage() + " - " + exc.getStackTrace()
                        };        		
        	}
        }
    }