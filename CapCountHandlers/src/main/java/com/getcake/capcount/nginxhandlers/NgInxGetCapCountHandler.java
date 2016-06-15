package com.getcake.capcount.nginxhandlers;

import static nginx.clojure.MiniConstants.CONTENT_TYPE;
import static nginx.clojure.MiniConstants.NGX_HTTP_BAD_REQUEST;
import static nginx.clojure.MiniConstants.NGX_HTTP_INTERNAL_SERVER_ERROR;
import static nginx.clojure.MiniConstants.NGX_HTTP_OK;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.capcount.model.ReadCapCountRequest;
import com.getcake.capcount.services.CapCountService;
import com.getcake.capcount.services.CapCountWebService;
import com.getcake.util.AwsUtil;
import com.getcake.util.CakeCommonUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public class NgInxGetCapCountHandler implements NginxJavaRingHandler {

	private static final Logger logger = Logger.getLogger(NgInxGetCapCountHandler.class);
	
	private static CapCountService capCountService;
	
    private ObjectMapper jsonMapper;
	
	public NgInxGetCapCountHandler () {
		Properties properties;
		Config topLevelClientsConfig;
		String clientConfigFileName, currentDir = null, absCurrPath = null;
		
		try {				
			if (capCountService != null) {
				return;
			}
			
			String current = new java.io.File( "." ).getCanonicalPath();
	        currentDir = System.getProperty("user.dir");
	        Path currentRelativePath = Paths.get("");
	        absCurrPath = Paths.get("").toAbsolutePath().toString();
			properties = AwsUtil.loadProperties("cap-init-req-count-handlers.conf.qa.sparkMaster");
			
		    clientConfigFileName =  Paths.get("").toAbsolutePath().toString() + "/client-meta-data.conf";
			topLevelClientsConfig = ConfigFactory.parseFile(new File(clientConfigFileName));		        
		    capCountService = CapCountService.getInstance(properties, topLevelClientsConfig, true, null);
		    jsonMapper = capCountService.getJsonMapper();
		} catch (Throwable exc) {
			logger.error("absCurrPath: " + absCurrPath + " - currentDir: " + currentDir, exc);				
		}		
	}
	
	@Override
	public Object[] invoke(Map<String, Object> request) throws IOException {
		
    	String jsonData = null, command, query_string;
    	ReadCapCountRequest readCapCountRequest = null;
    	StringTokenizer strTokenizer;
    	
    	try {
        	command = (String)request.get("request-method");
        	if (! "get".equalsIgnoreCase(command)) {
                return new Object[] { 
                    	NGX_HTTP_BAD_REQUEST,
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        "HTTP verb not supported for " + command
                        };        		
        	}
        	
        	query_string = (String)request.get("query-string");
        	if (query_string == null || query_string.trim().length() == 0) {
                return new Object[] { 
                    	NGX_HTTP_BAD_REQUEST,
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        "missing get parameter"
                        };        		
        	}
        	
    		strTokenizer = new StringTokenizer (query_string, "=");
    		jsonData = strTokenizer.nextToken();
    		jsonData = strTokenizer.nextToken();
        	
    		if (jsonData == null || jsonData.trim().length() == 0) {
                return new Object[] { 
                	NGX_HTTP_BAD_REQUEST,
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                    "missing get parameter"
                    };
    		}
    	} catch (Throwable exc) {
    		logger.error("", exc);
            return new Object[] { 
            		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                    CakeCommonUtil.convertExceptionToString (exc) };        		
    	}
    	
		try {
    		readCapCountRequest = jsonMapper.readValue(jsonData, ReadCapCountRequest.class);        		
    	} catch (Throwable exc) {
    		logger.error("jsonData: " + jsonData, exc);
            return new Object[] { 
            	NGX_HTTP_BAD_REQUEST,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                CapCountWebService.REST_WS_INVALID_JSON_DATA_RESP };
    	}
		
    	try {
    		readCapCountRequest = capCountService.getCapCounts(readCapCountRequest);        		
            return new Object[] { 
                NGX_HTTP_OK,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), CapCountWebService.REST_WS_OK_RESP,                       		
                jsonMapper.writeValueAsString(readCapCountRequest)};        			
            
    	} catch (Throwable exc) {
    		logger.error("readCapCountRequest: " + readCapCountRequest, exc);    		
            return new Object[] { 
                NGX_HTTP_OK,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                exc.getLocalizedMessage()
                };    		
    	}
	}
}
