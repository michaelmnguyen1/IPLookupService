package com.getcake.geo.model;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class NullValueSerializer extends JsonSerializer<Object> {
	public static final String EMPTY_STRING = "";


	public void serialize(Object nullKey, JsonGenerator jsonGenerator,
			SerializerProvider unused) throws IOException,
			JsonProcessingException {
		jsonGenerator.writeString(EMPTY_STRING);
	}
}

