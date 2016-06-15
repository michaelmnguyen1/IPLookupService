/*
 * Michael M. Nguyen
 */
package com.getcake.geo.nginxhandler;

/**
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
import com.getcake.util.CakeCommonUtil;

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
                    return new Object[] { 
                            NGX_HTTP_OK, 
                            ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                            jsonMapper.writeValueAsString(GeoIPLookupHandler.geoController.getGeoInfoStatistics())                        		
                            };        			
        		}
        		
                return new Object[] { 
                        NGX_HTTP_OK, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        jsonMapper.writeValueAsString(GeoIPLookupHandler.geoController.resetGeoInfoStatistics())                        		
                        };
        		
        		
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        CakeCommonUtil.convertExceptionToString (exc) };        		
        	}
        }
    }