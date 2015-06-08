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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import test.TestGetSetBenchmark;

import com.getcake.geo.model.*;
import com.getcake.geo.service.GeoService;

/**
 * Handles requests for the Employee service.
 */
@RestController
public class GeoController {
	
	private static final Logger logger = LoggerFactory.getLogger(GeoController.class);
	
    @Autowired
    public GeoService geoService;

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

    @RequestMapping(value = "load", method = RequestMethod.GET)
    public @ResponseBody long loadHashCacheDao(
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag,
    	@RequestParam(value="topnumrows", defaultValue="-1") long topNumRows) throws Throwable  {
    	
    	logger.debug("/internalMem/load v1");
    	return geoService.loadHashCacheDao(flushCacheFlag, topNumRows);
    }

    private LoadStatistics loadStatistics; 
    
    @RequestMapping(value = "geo", method = RequestMethod.GET)
    public @ResponseBody GeoInfo getGeoInfo(@RequestParam(value="ipaddress", defaultValue="") String ipAddress) {
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
    
    @RequestMapping(value = "statistics", method = RequestMethod.GET)
    public @ResponseBody LoadStatistics getGeoInfoStatistics() {
    	loadStatistics.avgDurationNanoSec = loadStatistics.accDuration / (double)loadStatistics.count;
    	loadStatistics.avgDurationMicroSec = loadStatistics.avgDurationNanoSec / 1000;
    	loadStatistics.avgDurationMilliSec = loadStatistics.avgDurationMicroSec / 1000;
    	return loadStatistics;
    }    
    
    @RequestMapping(value = "statistics", method = RequestMethod.PUT)
    public @ResponseBody LoadStatistics resetGeoInfoStatistics() {
    	loadStatistics.accDuration = 0;
    	loadStatistics.count = 0;
    	loadStatistics.maxDuration = 0;
    	loadStatistics.minDuration = Long.MAX_VALUE;
    	loadStatistics.avgDurationNanoSec = 0;
    	return loadStatistics;
    }    
    
    @RequestMapping(value = "locid", method = RequestMethod.GET)
    public @ResponseBody long getLocationId(
    	@RequestParam(value="ipaddress", defaultValue="0") String ipAddress) {
    	
		long startTime, endTime, locationId;
    	// logger.debug("/locid");
		startTime = Calendar.getInstance().getTimeInMillis();
		locationId = geoService.getLocationId(ipAddress);
		endTime = Calendar.getInstance().getTimeInMillis();
    	// logger.debug("/locid locationId: " + locationId + " dur(ms): " + (endTime - startTime));		
		return locationId;
    }
	
    @RequestMapping(value = "ipv4numnodes", method = RequestMethod.GET)
    public @ResponseBody long getIpv4NumNodes() {
    	
    	logger.debug("/ipv4numnodes");
    	return geoService.getIpv4NumNodes();
    }

    @RequestMapping(value = "ipv6numnodes", method = RequestMethod.GET)
    public @ResponseBody long getIpv6NumNodesAll() {
    	
    	logger.debug("/ipv6numnodes");
    	return geoService.getIpv6NumNodes();
    }

    @RequestMapping(value = "status", method = RequestMethod.GET)
    public @ResponseBody String getStatus() {    	
    	return "OK";
    }

    private long statusCount = 0;
    @RequestMapping(value = "statuscount", method = RequestMethod.GET)
    public @ResponseBody long getStatusCount() {    	
    	return ++statusCount;
    }

    /*    
    @RequestMapping(value = "/locid/loadtest", method = RequestMethod.GET)
    public @ResponseBody String getLocationIddLoadtest (
       	@RequestParam(value="ipaddress", defaultValue="0") String ipAddress,
    	@RequestParam(value="numops", defaultValue="1") int numOps) {
    	
		long startTime, endTime, locationId = 0, duration;
		float rate;
		String response;
		
    	logger.debug("/locid/loadtest");
		startTime = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < numOps; i++) {
			locationId = geoService.getLocationId(ipAddress);			
		}
		endTime = Calendar.getInstance().getTimeInMillis();
		duration = endTime - startTime;
		if (duration > 0) {
			rate = numOps / duration;
			response = "loadtest locationId: " + locationId  + " numOps: " + numOps + 
	        		" dur(ms): " + duration + " - rate:" + rate;		
		} else {
	    	response = "loadtest locationId: " + locationId  + " numOps: " + numOps + 
	        		" dur(ms): " + duration + " - rate: infinite";					
		}
		return response;
    }

    private LoadStatistics pauseStatistics; 
    private long pauseDuration = 10000;

    @RequestMapping(value = "pauseduration", method = RequestMethod.GET)
    public @ResponseBody long setPauseDuration (
    		@RequestParam(value="pauseduration") long pauseDuration) throws InterruptedException {
    	this.pauseDuration = pauseDuration; 
    	return this.pauseDuration;
    }
    
    @RequestMapping(value = "pausetest", method = RequestMethod.GET)
    public @ResponseBody long pauseTest () throws InterruptedException {
    	GeoInfo geoInfo;
    	long startTime, duration;
    	// logger.debug("/geo: " + ipAddress);
		startTime = Calendar.getInstance().getTimeInMillis();
    	Thread.currentThread().sleep(10000);
    	duration = Calendar.getInstance().getTimeInMillis() - startTime;
    	if (duration > pauseStatistics.maxDuration) {
    		pauseStatistics.maxDuration = duration;
    	}
    	if (duration < pauseStatistics.minDuration) {
    		pauseStatistics.minDuration = duration;
    	}
    	pauseStatistics.accDuration += duration;
    	pauseStatistics.count++;
    	return pauseStatistics.count;
    }
    
    @RequestMapping(value = "pausestatistics", method = RequestMethod.GET)
    public @ResponseBody LoadStatistics getPauseStatistics() {
    	pauseStatistics.avgDuration = (float)pauseStatistics.accDuration / (float)pauseStatistics.count;
    	return pauseStatistics;
    }    
    
    @RequestMapping(value = "pausestatistics", method = RequestMethod.PUT)
    public @ResponseBody LoadStatistics resetPauseStatistics() {
    	pauseStatistics.accDuration = 0;
    	pauseStatistics.count = 0;
    	pauseStatistics.maxDuration = 0;
    	pauseStatistics.minDuration = Long.MAX_VALUE;
    	pauseStatistics.avgDuration = 0;
    	return pauseStatistics;
    }    
    
    
    @RequestMapping(value = "/geo/loadtest", method = RequestMethod.GET)
    public @ResponseBody String getGeoInfoLoadTest(
       	@RequestParam(value="ipaddress", defaultValue="0") String ipAddress,
    	@RequestParam(value="numops", defaultValue="1") int numOps) {
    	
    	GeoInfo geoInfo = null;
		String response;
		long startTime, endTime, locationId = 0, duration;
		float rate;
    	logger.debug("/geo/loadtest");
		startTime = Calendar.getInstance().getTimeInMillis();
		for (int i = 0; i < numOps; i++) {
			geoInfo = geoService.getGeoInfo(ipAddress);			
		}
		endTime = Calendar.getInstance().getTimeInMillis();
		duration = endTime - startTime;
		if (duration > 0) {
			rate = numOps / duration; 			
	    	response = "loadtest locationId: " + locationId  + " numOps: " + numOps + 
	        		" dur(ms): " + duration + " - rate:" + rate;		
		} else {
	    	response = "loadtest locationId: " + locationId  + " numOps: " + numOps + 
	        		" dur(ms): " + duration + " - rate: infinite";					
		}
		return response;
    }
    */    

	/*
    @RequestMapping(value = "directgeo", method = RequestMethod.GET)
    public @ResponseBody GeoInfo getGeoInfoDirect(@RequestParam(value="ipv6", defaultValue="") String ipv6) {
    	GeoInfo geoInfo;
    	
    	logger.debug("/directgeo ipv6: " + ipv6);
    	return geoService.getGeoInfoDirect(null, ipv6);
    }
    
    @RequestMapping(value = "msqlgeo", method = RequestMethod.GET)
    public @ResponseBody GeoInfo getGeoInfoByMsSql(@RequestParam(value="ipv6", defaultValue="") String ipv6) {
    	GeoInfo geoInfo;
    	
    	logger.debug("/msqlgeo ipv6: " + ipv6);
    	return geoService.getGeoInfoByMsSql(null, ipv6);
    }

    @RequestMapping(value = "internalMem/loadDummyData", method = RequestMethod.GET)
    public @ResponseBody int loadloadCacheHashCacheDao(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/internalMem/load");
    	geoService.loadDummyDataHashCacheDao(flushCacheFlag, numOps);
    	return numOps;
    }
    
	@RequestMapping(value =  "/rest/emp/dummy", method = RequestMethod.GET)
	public @ResponseBody Employee getDummyEmployee() {
		logger.info("Start getDummyEmployee");
		Employee emp = new Employee();
		emp.setId(9999);
		emp.setName("Dummy");
		emp.setCreatedDate(new Date());
		empData.put(9999, emp);
		return emp;
	}
	
	@RequestMapping(value = "/rest/emp/dummy4", method = RequestMethod.GET)
	public @ResponseBody Employee getDummyEmployee2() {
		logger.info("Start getDummyEmployee dummy4");
		Employee emp = new Employee();
		emp.setId(9999);
		emp.setName("Dummy4");
		emp.setCreatedDate(new Date());
		empData.put(9999, emp);
		return emp;
	}
		
    @RequestMapping(value = "/rest/emp/dummy2", method = RequestMethod.GET)
    public @ResponseBody GeoInfo getGeo2(@RequestParam(value="name", defaultValue="World") String name) {
    	GeoInfo geoInfo;
    	
    	logger.debug("EmployeeController: /rest/emp/dummy2 ");
    	geoInfo = new GeoInfo ();
    	geoInfo.setId(3);
    	geoInfo.setCountry("US");
    	return geoInfo;
    }
    */
	
    /*
    @RequestMapping(value = "geo/insertLookup", method = RequestMethod.GET)
    public @ResponseBody int insertGeoLookup(
    	@RequestParam(value="lowerImportId", defaultValue="0") String lowerImportId,
    	@RequestParam(value="upperImportId", defaultValue="10") String upperImportId) {
    	GeoInfo geoInfo;
    	
    	logger.debug("/geo/insertLookup: ");
    	return geoService.insertGeoLookup(Integer.parseInt(lowerImportId), Integer.parseInt(upperImportId));
    }
    
    @RequestMapping(value = "testredis", method = RequestMethod.GET)
    public @ResponseBody int testRedis(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/testredis ");
    	TestGetSetBenchmark.testGetSetBenchmark (flushCacheFlag, numOps);
    	return numOps;
    }
    
    @RequestMapping(value = "eleasticache/load", method = RequestMethod.GET)
    public @ResponseBody int loadEleasticache(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/redis/load");
    	geoService.loadEleasticache(flushCacheFlag);
    	return numOps;
    }
    
    @RequestMapping(value = "redis/load", method = RequestMethod.GET)
    public @ResponseBody int loadLocalRedisCache(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/redis/load");
    	geoService.loadLocalRedisCache(flushCacheFlag);
    	return numOps;
    }
    

    
    @RequestMapping(value = "internalmem/load/bytes1", method = RequestMethod.GET)
    public @ResponseBody int loadCacheIpv4FirstOneByte(
        	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag,
        	@RequestParam(value="topnumrows", defaultValue="10") long topNumRows) {
    	
    	logger.debug("/internalMem/load/bytes1 v1");
    	return geoService.loadCacheIpv4FirstNByte(flushCacheFlag, topNumRows);
    }

    @RequestMapping(value = "internalMem/load/bytes2", method = RequestMethod.GET)
    public @ResponseBody int loadCacheIpv4FirstTwoBytes(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/internalMem/load");
    	geoService.loadCacheIpv4FirstTwoBytes(flushCacheFlag);
    	return numOps;
    }

    @RequestMapping(value = "internalMem/load/bytes3", method = RequestMethod.GET)
    public @ResponseBody int loadCacheIpv4FirstThreeBytes(
    	@RequestParam(value="numops", defaultValue="10") int numOps,
    	@RequestParam(value="flushcache", defaultValue="true") boolean flushCacheFlag) {
    	
    	logger.debug("/internalMem/load");
    	geoService.loadCacheIpv4FirstTwoBytes(flushCacheFlag);
    	return numOps;
    }
	*/
    
	
}
