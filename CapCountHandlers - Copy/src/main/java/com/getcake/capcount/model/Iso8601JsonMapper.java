package com.getcake.capcount.model;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.Time;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class Iso8601JsonMapper extends ObjectMapper {

	private static final Logger logger = Logger.getLogger(Iso8601JsonMapper.class);
	static private Iso8601JsonMapper instance;
	
	static {
		try {
			instance = new Iso8601JsonMapper (); 			
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}
	
	static public Iso8601JsonMapper getInstance () {
		return instance;
	}
	
	private Iso8601JsonMapper () {
        configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false);						        
        
		TimeZone tz = TimeZone.getTimeZone("PST");
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
		dateFormat.setTimeZone(tz);
        
        SimpleModule module = new SimpleModule("Spark Stream Time Module");
        SparkStreamTimeSerializer sparkStreamTimeSerializer = new SparkStreamTimeSerializer(Time.class);
        module.addSerializer(sparkStreamTimeSerializer);	
        registerModule(module); 
        
        module = new SimpleModule("Spark Stream Time Deser Module");
        SparkStreamTimeDeserializer sparkStreamTimeDeserializer = new SparkStreamTimeDeserializer(Time.class);
        module.addDeserializer(Time.class, sparkStreamTimeDeserializer);	
        registerModule(module); 		
	}
}
