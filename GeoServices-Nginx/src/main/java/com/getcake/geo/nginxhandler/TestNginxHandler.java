package com.getcake.geo.nginxhandler;

/**
 * Hello world!
 *
 */

import static nginx.clojure.MiniConstants.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.*;

import com.getcake.geo.controller.GeoController;
import com.getcake.util.CakeCommonUtil;

import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public  class TestNginxHandler implements NginxJavaRingHandler {

		public Object [] retObjs;
		static int invokeCount = 0, constructorCount = 0;
		
		private static final Logger logger = Logger.getLogger(TestNginxHandler.class);
		
		public TestNginxHandler () {
			
			try {
				++constructorCount;
				System.out.println ("App constructor constructorCount: " + constructorCount);
				logger.debug("App constructor constructorCount: " + constructorCount);
				retObjs = new Object [3];
				retObjs[0] = NGX_HTTP_OK;
				retObjs[1] = ArrayMap.create(CONTENT_TYPE, "text/plain");
				retObjs[2] = invokeCount;
				
			} catch (Throwable exc) {
				exc.printStackTrace();
				logger.error("", exc);				
			}
		}

	    public static void main(String[] args) {
			
		}
		
        // @Override
        public Object[] invoke(Map<String, Object> request) {
        	try {
    			// System.out.println ("Public App invoke constructorCount: " + constructorCount);
    			// logger.debug("App invoke constructorCount: " + constructorCount);
    			
            	++invokeCount;
            	/*
    			System.out.println ("App invoke invokeCount: " + invokeCount);
    			logger.debug("App invoke invokeCount: " + invokeCount);
    			
            	StringBuilder msg = new StringBuilder ();
            	msg.append("constructorCount: " + constructorCount); 
            	msg.append("invokeCount: " + invokeCount + " - "); 
            	
            	/*
    			System.out.println ("App invoke invokeCount: " + invokeCount);
    			System.out.println ("retObjs[2]: " + retObjs[2]);
            	return  retObjs;
                */
            	
            	/*
            	if (retObjs == null) {
            		msg.append("retObjs == null");
                    return new Object[] { 
                            NGX_HTTP_OK, //http status 200
                            ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                            "Java & Nginx msg: " + msg + " - invokeCount:" + invokeCount + " - retObjs:" + retObjs //response body can be string, File or Array/Collection of them
                            };        		
            	} else {
                	if (retObjs[0] == null) {
                		msg.append(" - retObjs[0] == null");            		
                	} else {
                		msg.append(" - retObjs[0]:" + retObjs[0]);            		            		
                	}
                	
                	if (retObjs[1] == null) {
                		msg.append(" - retObjs[1] == null");            		
                	} else {
                		msg.append(" - retObjs[1]:" + retObjs[1]);            		            		
                	}
                	
                	if (retObjs[2] == null) {
                		msg.append(" - retObjs[2] == null");            		
                	} else {
                		msg.append(" - retObjs[2]:" + retObjs[2]);            		            		
                	}
                	retObjs[2] = invokeCount;
                	// return retObjs;
            	}
            	
                return new Object[] { 
                    NGX_HTTP_OK, //http status 200
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                    "Java & Nginx msg: " + msg + " - invokeCount:" + invokeCount + " - retObjs:" + retObjs //response body can be string, File or Array/Collection of them
                    };
                */
                return new Object[] { 
                        NGX_HTTP_OK, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        "" + invokeCount //response body can be string, File or Array/Collection of them
                        };
        	} catch (Throwable exc) {
        		logger.error("", exc);
                return new Object[] { 
                		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                        CakeCommonUtil.convertExceptionToString (exc) };        		
        	}
        }
    }