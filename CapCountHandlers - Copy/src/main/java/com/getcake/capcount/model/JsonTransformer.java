package com.getcake.capcount.model;

import java.io.Serializable;

import com.fasterxml.jackson.databind.ObjectMapper;

import spark.ResponseTransformer;

public class JsonTransformer  implements ResponseTransformer, Serializable {

	private ObjectMapper jsonMapper = new ObjectMapper();
	
   @Override
    public String render(Object model) {
	   try {
	   		return jsonMapper.writeValueAsString (model);		   
	   } catch (Throwable exc) {
		   return "JSON transform Error";
	   }
    }	
   
   public void setObjectMapper (ObjectMapper jsonMapper) {
	   this.jsonMapper = jsonMapper;
   }
   
   public ObjectMapper getObjectMapper () {
	   return jsonMapper;
   }
}
