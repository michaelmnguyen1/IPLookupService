/*
 * Michael M. Nguyen
 */
package com.getcake.geo.nginxhandler;

/**
 *
 */

import static nginx.clojure.MiniConstants.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.log4j.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.controller.GeoController;
import com.getcake.util.CakeCommonUtil;

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class GeoStatusHandler implements NginxJavaRingHandler {

		public Object [] retObjs;
		static int invokeCount = 0, constructorCount = 0;
		
		private static final Logger logger = Logger.getLogger(GeoStatusHandler.class);
		private ObjectMapper jsonMapper;
				
		public GeoStatusHandler () {
			
			try {
				jsonMapper = new ObjectMapper();
			} catch (Throwable exc) {
				exc.printStackTrace();
				logger.error("", exc);				
			}
		}

	    public static void main(String[] args) {
		}
		
        // @Override
        public Object[] invoke(Map<String, Object> request) {
    		File tmpFile;
    		FileInputStream fileInputStream = null;
    		String line, healthCheckResults;
    		StringBuilder healtResp;
    		BufferedReader reader = null;
    		
        	try {
            	String uplbFileName;
                String absCurrPath = Paths.get("").toAbsolutePath().toString();            	
        	    uplbFileName = absCurrPath + "/uplb.txt";
    	    	tmpFile = new File(uplbFileName); 
    	    	if (! tmpFile.exists()) {
    	    		logger.debug(uplbFileName + " does not exist");
                    return new Object[] { 
                		NGX_HTTP_SERVICE_UNAVAILABLE, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        ""                        		
                        };
    	    	}
        	    
    			fileInputStream = new FileInputStream (uplbFileName);
    	        reader = new BufferedReader(new InputStreamReader(fileInputStream));
	            line = reader.readLine();
	            
        		healthCheckResults = GeoIPLookupHandler.geoController.healthCheck();
                return new Object[] { 
                    NGX_HTTP_OK, 
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), healthCheckResults                       		
                    };        			
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, 
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        exc.getMessage()};        		
        	} finally {
    			try {
            		if (fileInputStream != null) {
    					fileInputStream.close();
            		}
            		if (reader != null) {
            			reader.close();
            		}
				} catch (IOException exc2) {
					logger.error("", exc2);
				}
        	} 
        }
    }