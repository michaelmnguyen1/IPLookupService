package com.getcake.geo.controller;

import static spark.Spark.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import spark.Spark;


public class GeoSpark {

	private static final Logger logger = LoggerFactory.getLogger(GeoSpark.class);
	private static GeoController geoController;
	
    public static void main(String[] args) {

    	JsonTransformer jsonTransformer;
    	ApplicationContext applicationContext;
    	int minthreads, maxthreads, idleTimeoutMillis;
    	
    	try {    		
    		applicationContext = new ClassPathXmlApplicationContext("application-context.xml");    		
    		geoController = applicationContext.getBean("geoController", GeoController.class);    			
        	logger.debug("");
        	logger.debug("GeoSpark v0.1 loadHashCacheDao start");
        	
    		geoController.loadHashCacheDao(false, -1);
        	logger.debug("GeoSpark v1 loadHashCacheDao done");
    		 
        	minthreads = Integer.parseInt(args[0]);
        	maxthreads = Integer.parseInt(args[1]);
        	idleTimeoutMillis = Integer.parseInt(args[2]);
        	logger.debug("GeoSpark v1 minthreads:" + minthreads + " - maxthreads:" + maxthreads +
        			" - idleTimeoutMillis:" + idleTimeoutMillis);
        	Spark.port(8080);
        	Spark.threadPool(maxthreads, minthreads, idleTimeoutMillis);
        	
        	jsonTransformer = new JsonTransformer ();
        	
            get("/geoservices/test", (request, response) -> {
                return "test ok";
            });

            get("/geoservices/status", (request, response) -> {
                return geoController.getStatus();
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
            		return "Error";
            	}
            }, jsonTransformer);
    		
            get("/geoservices/geo", (request, response) -> {            	
                return geoController.getGeoInfo(request.queryParams("ipaddress"));
            }, jsonTransformer);
    		
    	} catch (Throwable exc) {
    		logger.error("", exc);
    	}
    }
    
}
