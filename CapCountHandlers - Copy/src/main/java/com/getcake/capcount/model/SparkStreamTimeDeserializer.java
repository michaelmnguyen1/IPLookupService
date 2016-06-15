package com.getcake.capcount.model;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.*;

public class SparkStreamTimeDeserializer extends StdDeserializer<Time> implements Serializable {

	private static final Logger logger = Logger.getLogger(SparkStreamTimeDeserializer.class);
	
	private ThreadLocal<DateFormat> threadLocalDateFormat; 
	
	public SparkStreamTimeDeserializer(Class<Time> t) {		
		super(t);
		threadLocalDateFormat = new ThreadLocal<DateFormat>();
		threadLocalDateFormat.set(SparkStreamTimeSerializer.createDateFormat ());
	}

	@Override
	public Time deserialize(JsonParser jsonParser, DeserializationContext ctxt)	throws IOException, JsonProcessingException {
		 Date date;
		 Time sparkTime = null;
		 DateFormat dateFormat = null;
		 
		 if (jsonParser.getCurrentTokenId() == JsonTokenId.ID_STRING) {
            try { 
            	dateFormat = threadLocalDateFormat.get();
        		if (dateFormat == null) {
        			logger.warn("deserialize Thread Id: " + Thread.currentThread().getId() + " date format is null");
        			dateFormat = SparkStreamTimeSerializer.createDateFormat ();
        			threadLocalDateFormat.set(dateFormat);
        		}
            	date = dateFormat.parse(jsonParser.getText());
            	sparkTime = new Time (date.getTime());
            	return  sparkTime;
            } catch (Throwable exc) {
            	logger.error("");
            	logger.error("Exception with jsonParser.getText(): " + jsonParser.getText(), exc);            	
                return super.deserialize(jsonParser, ctxt, sparkTime);
            }
        } else {
            return super.deserialize(jsonParser, ctxt, sparkTime);
        }
	}
}
