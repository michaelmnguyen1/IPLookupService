package com.getcake.capcount.services;

import static spark.Spark.post;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext; 
import spark.Spark;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.capcount.model.JsonTransformer;
import com.getcake.capcount.model.SparkInitializeCapCountRequest;
import com.getcake.util.AwsUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
 
public final class CapCountInitializeRequestHandler  implements Serializable {
			
	private static final long serialVersionUID = 1L;

	private static final String 
		REST_WS_OK_RESP = "{\"status\":\"ok\"}".replace('\\', ' '),
		REST_WS_INVALID_JSON_DATA_RESP = "{\"status\":\"Invalid JSON Data\"}".replace('\\', ' '),
		REST_WS_NO_JSON_DATA_RESP = "{\"status\":\"No JSON Data\"}".replace('\\', ' ');
	
	private static final Logger logger = Logger.getLogger(CapCountInitializeRequestHandler.class);
	public static JavaSparkContext javaSparkContext;

	private Properties properties;
	private Config topLevelClientsConfig;
	private String initCapSparkInitializeCapUri, initCapSparkWebServiceRootUri;	
	private int driverCount = 0, runCount = 0;  
	private JsonTransformer jsonTransformer;	
    private int requestCount = 0;
	private boolean stopFlag = false, initializeCapCountReq;

	public static void main (String[] args) throws Exception { 
	
		String sparkMaster, appName, sparkConfigFileName, clientConfigFileName;
		CapCountInitializeRequestHandler capCountInitializeRequestHandler;
		
		try {
			
			if (args.length < 0) {
				System.err.println(
				  "Usage: BasicAvgMapPartitions <spark-config-file>.\n"					  
				);
				System.exit(1);    	
			}
		
			sparkConfigFileName = args[0];
			
			capCountInitializeRequestHandler = new CapCountInitializeRequestHandler();
						
			capCountInitializeRequestHandler.properties = AwsUtil.loadProperties(sparkConfigFileName);
		
			clientConfigFileName = args[1];
			capCountInitializeRequestHandler.topLevelClientsConfig = ConfigFactory.parseFile(new File(clientConfigFileName));
		
			sparkMaster = capCountInitializeRequestHandler.properties.getProperty("spark.master"); 
			appName = capCountInitializeRequestHandler.properties.getProperty("spark.app.name"); 
		
			capCountInitializeRequestHandler.run(sparkMaster, appName, capCountInitializeRequestHandler.properties, 
				capCountInitializeRequestHandler.topLevelClientsConfig); 			
		} catch (Throwable exc) {
			logger.error("", exc);
			System.exit(-1);
		}
	} 

	public void run(String sparkMaster, String appName, Properties properties, Config topLevelClientsConfig) throws Throwable {	    
		SparkConf sparkConfig = new SparkConf();
	
		if (sparkMaster.indexOf("local") >= 0) {
			sparkConfig.setMaster(sparkMaster); 		    
			sparkConfig.setAppName(appName);
			sparkConfig.set("spark.executor.memory", properties.getProperty("spark.executor.memory"));
			sparkConfig.set("spark.driver.memory", properties.getProperty("spark.driver.memory"));
		} 	    
	
		javaSparkContext = new JavaSparkContext(sparkConfig);	      
	
		CapCountWebService capCountWebService;
		CapCountService capCountService;
	
		capCountWebService = CapCountWebService.getInstance(properties, topLevelClientsConfig, false);
		capCountService = CapCountService.getInstance(properties, topLevelClientsConfig, false, capCountWebService);
		
		jsonTransformer = new JsonTransformer ();
		ObjectMapper jsonMapper;
		jsonMapper = capCountService.getJsonMapper();
		jsonTransformer.setObjectMapper(jsonMapper);
		
		int initCapSparkWebMinThreads, initCapSparkWebMaxThreads, initCapSparkWebIdleTimeoutMillis, initCapSparkWebPort; 
	
		initCapSparkWebServiceRootUri = properties.getProperty("initCapSparkWebServiceRootUri"); 
		initCapSparkInitializeCapUri = properties.getProperty("initCapSparkInitializeCapUri");
		logger.debug("initCapSparkWebServiceRootUri: " + initCapSparkWebServiceRootUri);
		logger.debug("initializeCapUri: " + initCapSparkInitializeCapUri);
		
		initCapSparkWebMinThreads = Integer.parseInt(properties.getProperty("initCapSparkWebMinThreads")); 
		initCapSparkWebMaxThreads = Integer.parseInt(properties.getProperty("initCapSparkWebMaxThreads")); 
		initCapSparkWebIdleTimeoutMillis = Integer.parseInt(properties.getProperty("initCapSparkWebIdleTimeoutMillis")); 
		initCapSparkWebPort = Integer.parseInt(properties.getProperty("initCapSparkWebPort"));		
		Spark.port(initCapSparkWebPort);
		Spark.threadPool(initCapSparkWebMaxThreads, initCapSparkWebMinThreads, initCapSparkWebIdleTimeoutMillis);

		post (initCapSparkWebServiceRootUri + "/stop", (request, response) -> {
			try { 
				logger.debug("received /caphandlerservices//stop exiting ");
				stopFlag = true;
				return "stopFlag set";        		
			} catch (Throwable exc) {
				logger.error("", exc);
				return exc.getLocalizedMessage();
			}
		}); 
	
		post (initCapSparkWebServiceRootUri + initCapSparkInitializeCapUri, (request, response) -> {
			String jsonData;
			SparkInitializeCapCountRequest initializeCapCountRequest = null;
	
			try {    	    	
				jsonData =  request.body();
        		if (jsonData == null || jsonData.trim().length() == 0) {
					return REST_WS_NO_JSON_DATA_RESP;        			
				}
				initializeCapCountRequest = jsonMapper.readValue(jsonData, SparkInitializeCapCountRequest.class);
				logger.debug("post /caphandlerservices/clients initializeCapCountReq: " + initializeCapCountRequest);			
			} catch (Throwable exc) {
				logger.error("", exc);
				return REST_WS_INVALID_JSON_DATA_RESP + exc.getLocalizedMessage();
			}
	
			try {
				capCountService.initializeCapCountFirstSteps_Spark(initializeCapCountRequest, ++runCount, 
					Thread.currentThread().getId());
				return REST_WS_OK_RESP + " requestCount: " + (++requestCount);
			} catch (Throwable exc) {
				logger.error("", exc);
				response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
				return exc.getLocalizedMessage() + " requestCount: " + (++requestCount);
			}
		});     
		
		while (!stopFlag) 
		{
			logger.debug("while loop continue for driverCount: " + ++driverCount );
			if (initializeCapCountReq) {
				logger.debug("while loop initializeCapCountReq: " + initializeCapCountReq + " process request");
	
				initializeCapCountReq = false;			
			} else {
				  synchronized (this) {
						try {
							this.wait();
						} catch (InterruptedException exc) {
							logger.error ("", exc);
						}				  
				  }			  
			}	    
		}
		javaSparkContext.stop();
		Spark.stop();			
	}
	
}