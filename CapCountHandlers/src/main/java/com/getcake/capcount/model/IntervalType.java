package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.getcake.capcount.dao.CapCountDao;

public enum IntervalType implements Serializable {

	Monthly ("ML", "Monthly"), Weekly ("WL", "Weekly"), Daily ("DL", "Daily"), Custom ("CT", "Custom");

	private static final Logger logger = Logger.getLogger(IntervalType.class);
	
	private static Map<String, IntervalType> intervalTypeMap;
	static {
		try {
			intervalTypeMap = new HashMap<>();
			intervalTypeMap.put(Monthly.code, Monthly);
			intervalTypeMap.put(Weekly.code, Weekly);
			intervalTypeMap.put(Daily.code, Daily);
			intervalTypeMap.put(Custom.code, Custom);
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}
	
	public static IntervalType getIntervalType (String code)  {
		IntervalType intervalType = intervalTypeMap.get(code);		
		if (intervalType != null) {
			return intervalType;
		}
		
		throw new RuntimeException ("Invalid IntervalType code of " + code);
	}
	
  	private String code, name;
	private IntervalType (String code, String name) {
		this.code = code;
		this.name = name;
	}
	
	public String toString () {
		return name;
	}

	public String getName () {
		return name;
	}
	
	public String getCode () {
		return code;
	}
	
}
