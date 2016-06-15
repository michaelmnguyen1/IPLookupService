package com.getcake.capcount.model;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.getcake.capcount.services.CapCountService;

import org.apache.spark.streaming.*;
import org.apache.log4j.Logger;

public class SparkStreamTimeSerializer extends StdSerializer<Time> implements Serializable {

	private static final Logger logger = Logger.getLogger(SparkStreamTimeSerializer.class);
	
	public static final String DateFormatStr = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX" ;
	public static final String Default_Time_Zone = "PST";
	private ThreadLocal<DateFormat> threadLocalDateFormat; 
	
	public SparkStreamTimeSerializer(Class<Time> t) {
		super(t);
		threadLocalDateFormat = new ThreadLocal<DateFormat>();
		threadLocalDateFormat.set(createDateFormat ());
	}

	public static DateFormat createDateFormat () {
		TimeZone tz = TimeZone.getTimeZone(Default_Time_Zone);
		DateFormat dateFormat = new SimpleDateFormat(DateFormatStr);
		dateFormat.setTimeZone(tz);
		return dateFormat;		
	}	
	
	@Override
	public void serialize(Time sparkStreamTime, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
			throws IOException, JsonGenerationException {
		
		try {
			Date date = Calendar.getInstance().getTime();
			date.setTime(sparkStreamTime.milliseconds());
			DateFormat dateFormat =  threadLocalDateFormat.get();
    		if (dateFormat == null) {
    			logger.warn("serialize Thread Id: " + Thread.currentThread().getId() + " date format is null");
    			dateFormat = createDateFormat ();
    			threadLocalDateFormat.set(dateFormat);
    		}
			jsonGenerator.writeString(dateFormat.format(date));			
		} catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
		}		
	}
	
	
}
