package com.getcake.capcount.nginxhandlers;

import static nginx.clojure.MiniConstants.CONTENT_TYPE;
import static nginx.clojure.MiniConstants.NGX_HTTP_BAD_REQUEST;
import static nginx.clojure.MiniConstants.NGX_HTTP_INTERNAL_SERVER_ERROR;
import static nginx.clojure.MiniConstants.NGX_HTTP_OK;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import org.apache.log4j.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.capcount.model.SparkIncrementCapCountRequest;
import com.getcake.capcount.services.CapCountService;
import com.getcake.capcount.services.CapCountWebService;
import com.getcake.util.AwsUtil;
import com.getcake.util.CakeCommonUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import nginx.clojure.java.ArrayMap;
import nginx.clojure.java.NginxJavaRingHandler;

public class NgInxIncrementCapCountHandler implements NginxJavaRingHandler {
	private static final Logger logger = Logger.getLogger(NgInxIncrementCapCountHandler.class);	
	private static CapCountService capCountService;
	
    private ObjectMapper jsonMapper;
	
	public NgInxIncrementCapCountHandler () {
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
    	String jsonData = null, command;
    	SparkIncrementCapCountRequest sparkIncrementCapCountReq = null;
    	InputStream inputStream;
    	Scanner scanner = null;
    	StringBuilder tmpStr;
    	
    	try {
        	command = (String)request.get("request-method");
        	if (! "post".equalsIgnoreCase(command)) {
                return new Object[] { 
                    	NGX_HTTP_BAD_REQUEST,
                        ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                        "HTTP verb not supported for " + command
                        };        		
        	}
        	
        	inputStream = (InputStream)request.get("body");
        	if (inputStream != null) {
            	scanner = new Scanner (inputStream,"UTF-8");
            	tmpStr = new StringBuilder ();
            	while (scanner.hasNext()) {
            		tmpStr.append(scanner.next());
            	}
            	jsonData = tmpStr.toString();
        	}
    		
    		if (jsonData == null) {
                return new Object[] { 
                	NGX_HTTP_BAD_REQUEST,
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                    "missing post body"
                    };
    		}
    	} catch (Throwable exc) {
    		logger.error("", exc);
            return new Object[] { 
            		NGX_HTTP_INTERNAL_SERVER_ERROR, //http status 200
                    ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                    CakeCommonUtil.convertExceptionToString (exc) };        		
    	} finally {
    		try {
        		if (scanner != null) {
            		scanner.close();    		    			
        		}    			
    		} catch (Throwable exc) {
    			logger.error("", exc);
    		}    		
    	}
    	
		try {
    		sparkIncrementCapCountReq = jsonMapper.readValue(jsonData, SparkIncrementCapCountRequest.class);        		
    	} catch (Throwable exc) {
    		logger.error("jsonData: " + jsonData, exc);
            return new Object[] { 
            	NGX_HTTP_BAD_REQUEST,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), 
                CapCountWebService.REST_WS_INVALID_JSON_DATA_RESP };
    	}
		
    	try {
    		capCountService.incrementCapCount(sparkIncrementCapCountReq);
            return new Object[] { 
                NGX_HTTP_OK,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), CapCountWebService.REST_WS_OK_RESP                       		
                };        			
            
    	} catch (Throwable exc) {
    		logger.error("sparkIncrementCapCountReq: " + sparkIncrementCapCountReq, exc);    		
            return new Object[] { 
                NGX_HTTP_OK,
                ArrayMap.create(CONTENT_TYPE, "text/plain"), //headers map
                exc.getLocalizedMessage()
                };    		
    	}
	}
}
