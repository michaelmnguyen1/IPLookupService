package com.getcake.geo.service;

import com.fasterxml.jackson.databind.ObjectMapper;

import spark.ResponseTransformer;

public class JsonTransformer  implements ResponseTransformer {

	private ObjectMapper mapper = new ObjectMapper();
	
   @Override
    public String render(Object model) {
	   try {
	   		return mapper.writeValueAsString (model);		   
	   } catch (Throwable exc) {
		   return "JSON transform Error";
	   }
    }	
}
