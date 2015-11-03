package com.getcake.geo.nginxhandler;

/**
 * Hello world!
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

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class GeoStatusHandler implements NginxJavaRingHandler {

		public Object [] retObjs;
		static int invokeCount = 0, constructorCount = 0;
		
		private static final Logger logger = Logger.getLogger(GeoStatusHandler.class);
				
		public GeoStatusHandler () {
			
			try {
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
		}
		
        // @Override
        public Object[] invoke(Map<String, Object> request) {
    		File tmpFile;
    		FileInputStream fileInputStream = null;
    		String line;
    		BufferedReader reader = null;
    		
        	try {
            	// logger.debug("ipAddress: " + ipAddress + " loc id: " + geoInfo.getLocationId());
        		// loadStatistics = GeoIPLookupApp.geoController.getGeoInfoStatistics();
        		
            	String uplbFileName;
                String absCurrPath = Paths.get("").toAbsolutePath().toString();            	
        	    uplbFileName = absCurrPath + "/uplb.txt";
    	    	tmpFile = new File(uplbFileName); 
    	    	if (! tmpFile.exists()) {
    	    		logger.debug(uplbFileName + " does not exist");
                    return new Object[] { 
                		NGX_HTTP_SERVICE_UNAVAILABLE, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        ""                        		
                        };
    	    	}
        	    
    			fileInputStream = new FileInputStream (uplbFileName);
    	    	// Read one text line at a time and display.
    	        reader = new BufferedReader(new 
    	        		InputStreamReader(fileInputStream));
	            line = reader.readLine();
	            if (line == null || !"up".equalsIgnoreCase(line)) {
    	    		logger.debug(uplbFileName + "  exists but contains " + line);
                    return new Object[] { 
                		NGX_HTTP_SERVICE_UNAVAILABLE, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        ""                        		
                        };	            	
	            } 
	            
                return new Object[] { 
                    NGX_HTTP_OK, //http status 200
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                    GeoIPLookupHandler.geoController.getStatus() + " micro-seconds"                        		
                    //response body can be string, File or Array/Collection of them
                    };
                
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        "Java exc: " + exc.getMessage() + " - " + exc.getStackTrace()
                        };        		
        	} finally {
    			try {
            		if (fileInputStream != null) {
    					fileInputStream.close();
            		}
            		if (reader != null) {
            			reader.close();
            		}
				} catch (IOException exc2) {
					// TODO Auto-generated catch block
					logger.error("", exc2);
				}
        	} 
        }
    }