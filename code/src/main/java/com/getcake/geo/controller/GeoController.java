package com.getcake.geo.controller;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.getcake.geo.model.*;
import com.getcake.geo.service.GeoService;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.ObjectMapper;
/*
import org.fasterxml.jackson.JsonGenerationException;
import org.fasterxml.jackson.map.JsonMappingException;
import org.fasterxml.jackson.map.ObjectMapper;
*/

/**
 * Handles requests for the Employee service.
 */
public class GeoController {
	
	private static final Logger logger = LoggerFactory.getLogger(GeoController.class);
	private static GeoController instance;
	    
    private GeoService geoService;
    private LoadStatistics loadStatistics; 
    
    public GeoController () {
    	loadStatistics = new LoadStatistics ();
    }
    
    public GeoService getGeoService () {
    	return geoService;
    }
    
    public void setGeoService (GeoService geoInfoService) {
    	this.geoService = geoInfoService;
    }
    	
	//Map to store employees, ideally we should use database
	Map<Integer, GeoInfo> empData = new HashMap<Integer, GeoInfo>();

    // @RequestMapping(value = "load", method = RequestMethod.GET)  // flushcache topnumrows
    public long loadHashCacheDao(
    	boolean flushCacheFlag, 
    	long topNumRows) throws Throwable  {
    	
    	logger.debug("/internalMem/load v1");
    	return geoService.loadHashCacheDao(flushCacheFlag, topNumRows);
    }

    // @RequestMapping(value = "geo", method = RequestMethod.GET) ipaddress
    public GeoInfo getGeoInfo(String ipAddress) {
    	GeoInfo geoInfo;
    	long startTime, duration;
    	// logger.debug("/geo: " + ipAddress);
		startTime = System.nanoTime();
    	geoInfo = geoService.getGeoInfo(ipAddress);
    	duration = System.nanoTime() - startTime;
    	if (duration > loadStatistics.maxDuration) {
    		loadStatistics.maxDuration = duration;
    	}
    	if (duration < loadStatistics.minDuration) {
    		loadStatistics.minDuration = duration;
    	}
    	// do not use synchronized on purpose for increments of accDuration and count to avoid impact on performance
    	// for the statistics purposes, these calculations do not need to be exact. 
    	loadStatistics.accDuration += duration;
    	loadStatistics.count++;
    	return geoInfo;
    }
    
    // @RequestMapping(value = "statistics", method = RequestMethod.GET) throws Throwable
    public LoadStatistics getGeoInfoStatistics()  {
    	loadStatistics.avgDurationNanoSec = loadStatistics.accDuration / (double)loadStatistics.count;
    	loadStatistics.avgDurationMicroSec = loadStatistics.avgDurationNanoSec / 1000;
    	loadStatistics.avgDurationMilliSec = loadStatistics.avgDurationMicroSec / 1000;
    	
    	// ObjectMapper mapper = new ObjectMapper();
    	// return mapper.writeValueAsString (loadStatistics);
    	return loadStatistics;
    }    
    
    // @RequestMapping(value = "statistics", method = RequestMethod.PUT)
    public LoadStatistics resetGeoInfoStatistics() {
    	loadStatistics.accDuration = 0;
    	loadStatistics.count = 0;
    	loadStatistics.maxDuration = 0;
    	loadStatistics.minDuration = Long.MAX_VALUE;
    	loadStatistics.avgDurationNanoSec = 0;
    	return loadStatistics;
    }    
    
    // @RequestMapping(value = "locid", method = RequestMethod.GET) ipaddress
    public long getLocationId(String ipAddress) {
    	
		long startTime, endTime, locationId;
    	// logger.debug("/locid");
		startTime = Calendar.getInstance().getTimeInMillis();
		locationId = geoService.getLocationId(ipAddress);
		endTime = Calendar.getInstance().getTimeInMillis();
    	// logger.debug("/locid locationId: " + locationId + " dur(ms): " + (endTime - startTime));		
		return locationId;
    }
	
    // @RequestMapping(value = "ipv4numnodes", method = RequestMethod.GET)
    public long getIpv4NumNodes() {
    	
    	logger.debug("/ipv4numnodes");
    	return geoService.getIpv4NumNodes();
    }

    // @RequestMapping(value = "ipv6numnodes", method = RequestMethod.GET)
    public long getIpv6NumNodes() {
    	
    	logger.debug("/ipv6numnodes");
    	return geoService.getIpv6NumNodes();
    }

    // @RequestMapping(value = "status", method = RequestMethod.GET)
    public String getStatus() {    	
    	return "OK";
    }

    private long statusCount = 0;
    // @RequestMapping(value = "statuscount", method = RequestMethod.GET)
    public long getStatusCount() {    	
    	return ++statusCount;
    }

	
}
