package com.getcake.capcount.services;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Properties;
import spark.Spark;
import static spark.Spark.get;
import static spark.Spark.put;
import static spark.Spark.post;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;
import com.getcake.capcount.model.Iso8601JsonMapper;
import com.getcake.capcount.model.JsonTransformer;
import com.getcake.capcount.model.ReadCapCountRequest;
import com.getcake.capcount.model.SparkIncrementCapCountRequest;
import com.getcake.capcount.model.SparkInitializeCapCountRequest;
import com.getcake.capcount.test.CapCountProducer;
import com.getcake.util.AwsUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class CapCountWebService {
	protected static final Logger logger = Logger.getLogger(CapCountWebService.class);
	
	public static final String VERSION = "0.01",
		REST_WS_OK_RESP = "{\"status\":\"ok\"}".replace('\\', ' '),
		REST_WS_INVALID_JSON_DATA_RESP = "{\"status\":\"Invalid JSON Data\"}".replace('\\', ' '),
		REST_WS_NO_JSON_DATA_RESP = "{\"status\":\"No JSON Data\"}".replace('\\', ' ');
	
	private static CapCountWebService instance;
		
	private Properties properties;
	private String apiSparkWebServicesUri, incrementCapUrl, incrementCapUri,  
		apiSparkWebServiceRootUri, apiInitializeCapUri, apiInitializeCapScheduledUri, apiInitializeCapBackfillUri, 
		apiInitializeCapScheduledUrl, apiInitializeCapBackfillUrl, 
		apiSaveOnlyInitializeCapUri, apiSaveOnlyInitializeCapUrl, apiGetCapCountUri;
	private JsonTransformer jsonTransformer;	
    private Iso8601JsonMapper jsonMapper;
    private String requestReceivedRegion;
	private CapCountService capCountService;

	static {
		try {
			logger.debug("CapCountWebService static code block");
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}
	
	public static CapCountWebService getInstance (Properties properties, Config topLevelClientsConfig, boolean initDaoFlag) {
		if (instance != null) {
			logger.debug("CapCountWebService instance != null");
			return instance;
		}
		
		synchronized (CapCountWebService.class) {
			if (instance != null) {
				logger.debug("CapCountWebService instance != null 2nd call");
				return instance;
			}
			logger.debug("CapCountWebService instance created");
			instance = new CapCountWebService (properties, topLevelClientsConfig, initDaoFlag);
			
			logger.debug("CapCountWebService instance started");
		}
		return instance;		
	}
	
	public static CapCountWebService getInstance (Properties properties, Config topLevelClientsConfig) {
		return getInstance (properties, topLevelClientsConfig, false);
	}
	
	private CapCountWebService (Properties properties, Config topLevelClientsConfig, boolean initDaoFlag) {
		init (properties, topLevelClientsConfig, initDaoFlag);
	}
	
	public CapCountService getCapCountService () {
		return capCountService;
	}
	
	private void init (Properties properties, Config topLevelClientsConfig, boolean initDaoFlag) {
		
	    capCountService = CapCountService.getInstance(properties, topLevelClientsConfig, initDaoFlag, this);
	    		
	    jsonTransformer = new JsonTransformer ();
	    jsonMapper = capCountService.getJsonMapper();
	    jsonTransformer.setObjectMapper(jsonMapper);
		this.properties = properties;
		
		apiSparkWebServicesUri = properties.getProperty("apiSparkWebServer");
		apiSparkWebServicesUri += ":";
		apiSparkWebServicesUri += properties.getProperty("apiSparkWebPort");
		apiSparkWebServiceRootUri = properties.getProperty("apiSparkWebServiceRootUri"); 
		apiSparkWebServicesUri += apiSparkWebServiceRootUri;
		incrementCapUri = properties.getProperty("incrementCapUri");
		incrementCapUrl = apiSparkWebServicesUri + incrementCapUri;		
		requestReceivedRegion = properties.getProperty("region");
		
		apiInitializeCapUri = properties.getProperty("apiInitializeCapUri");						
		apiInitializeCapScheduledUri = properties.getProperty("apiInitializeCapScheduledUri");				
		apiInitializeCapScheduledUrl =  apiSparkWebServicesUri + apiInitializeCapScheduledUri;				
		
		apiInitializeCapBackfillUri = properties.getProperty("apiInitializeCapBackfillUri");				
		apiInitializeCapBackfillUrl =  apiSparkWebServicesUri + apiInitializeCapBackfillUri;						
		
		apiSaveOnlyInitializeCapUri = properties.getProperty("apiSaveOnlyInitializeCapUri");
		apiSaveOnlyInitializeCapUrl = apiSparkWebServicesUri + apiSaveOnlyInitializeCapUri;
		
		apiGetCapCountUri =  properties.getProperty("apiGetCapCountUri");
		
		logger.debug("sparkWebServicesUri: " + apiSparkWebServicesUri);
		logger.debug("properties.getProperty(incrementCapUri): " +  properties.getProperty("incrementCapUri"));
		logger.debug("incrementCapUrl: " + incrementCapUrl);
		logger.debug("apiGetCapCountUri: " + apiGetCapCountUri);
	}
	
    public static void main(String[] args) {
    	String appPropFileName, clientMetaInfoFileName;
    	CapCountWebService capCountWebService;
    	
    	try {    		
        	if (args.length < 2 ) {
        		System.out.println ("usage: <app properties file> <client-meta-info.conf>");
        		return;
        	} 
        	
        	appPropFileName = args[0];
        	clientMetaInfoFileName = args[1];
        	Properties properties;
            Config topLevelClientsConfig;
    	    properties = AwsUtil.loadProperties (appPropFileName);
    		topLevelClientsConfig = ConfigFactory.parseFile(new File(clientMetaInfoFileName)); 
        	
        	capCountWebService = CapCountWebService.getInstance (properties, topLevelClientsConfig, true);
        	capCountWebService.run();
        	// capCountWebService.start(appPropFileName, clientMetaInfoFileName);    		
    	} catch (Throwable exc) {
    		logger.error("", exc);
			System.exit(-1);
    	}
    }

    private void run () throws Throwable {
    	int apiSparkWebMinThreads, apiSparkWebMaxThreads, apiSparkWebIdleTimeoutMillis, apiSparkWebPort = 8080; 
    	String msg;
    	
    	apiSparkWebMinThreads = Integer.parseInt(properties.getProperty("apiSparkWebMinThreads")); 
    	apiSparkWebMaxThreads = Integer.parseInt(properties.getProperty("apiSparkWebMaxThreads")); 
    	apiSparkWebIdleTimeoutMillis = Integer.parseInt(properties.getProperty("apiSparkWebIdleTimeoutMillis")); 
		apiSparkWebPort = Integer.parseInt(properties.getProperty("apiSparkWebPort"));
		logger.debug("apiSparkWebPort: " + apiSparkWebPort);
    	Spark.port(apiSparkWebPort);
    	Spark.threadPool(apiSparkWebMaxThreads, apiSparkWebMinThreads, apiSparkWebIdleTimeoutMillis);
    	
    	msg = "CapCountWebService " + VERSION + " - portNum: " + apiSparkWebPort + " - minthreads:" + apiSparkWebMinThreads + " - maxthreads:" + apiSparkWebMaxThreads + " - idleTimeoutMillis:" + apiSparkWebIdleTimeoutMillis;
    	logger.debug(msg);
    	            
		logger.debug("apiSparkWebServiceRootUri: " + apiSparkWebServiceRootUri);
        get(apiSparkWebServiceRootUri + "/test", (request, response) -> {
            return "test ok";
        });

        get(apiSparkWebServiceRootUri + "/health", (request, response) -> {
            try {
				return capCountService.getStatus();
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });

        get (apiSparkWebServiceRootUri + "/statuscount", (request, response) -> {
            return capCountService.getStatusCount();
        });

        get (apiSparkWebServiceRootUri + apiGetCapCountUri, (request, response) -> {
        	String jsonData;
        	ReadCapCountRequest readCapCountRequest = null;
        	
        	try {  
        		CapCountProducer.create_Read_ReqOffer_MicroEvent(jsonMapper, 77);
        		jsonData = request.queryParams("request");
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;        			
        		}
                readCapCountRequest = jsonMapper.readValue(jsonData, ReadCapCountRequest.class);
    			logger.debug("get from " + apiSparkWebServiceRootUri + apiGetCapCountUri + " : " + readCapCountRequest);
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	 
        	try {
        		readCapCountRequest = capCountService.getCapCounts(readCapCountRequest);        		
                return this.jsonMapper.writeValueAsString(readCapCountRequest);
        	} catch (Throwable exc) {
       	     	logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });     
        
        post (apiSparkWebServiceRootUri + apiInitializeCapUri, (request, response) -> {
        	String jsonData;
        	SparkInitializeCapCountRequest initializeCapCountRequest = null;
        	
        	try {    			
        		jsonData =  request.body();
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;
        		}
                initializeCapCountRequest = jsonMapper.readValue(jsonData, SparkInitializeCapCountRequest.class);
                initializeCapCountRequest.setRequestReceivedRegion(requestReceivedRegion);
    			logger.debug("post to " + apiSparkWebServiceRootUri + apiInitializeCapUri + " : " + initializeCapCountRequest);
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	 
        	try {
        		capCountService.initializeCapCountFirstSteps(initializeCapCountRequest, requestReceivedRegion);
                return REST_WS_OK_RESP;
        	} catch (Throwable exc) {
       	     	logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });     
        
        put (apiSparkWebServiceRootUri + apiInitializeCapScheduledUri, (request, response) -> {
        	String jsonData;
        	SparkInitializeCapCountRequest initializeCapCountRequest = null;
        	
        	try {    			
        		jsonData =  request.body();
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;        			
        		}
                initializeCapCountRequest = jsonMapper.readValue(jsonData, SparkInitializeCapCountRequest.class);
    			logger.debug("put /caphandlerservices/initializecap: " + initializeCapCountRequest);
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	 
        	try {
    			capCountService.initializeCapCountScheduled(initializeCapCountRequest);
                return REST_WS_OK_RESP;
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });     

        put (apiSparkWebServiceRootUri + apiInitializeCapBackfillUri, (request, response) -> {
        	String jsonData;
        	SparkInitializeCapCountRequest initializeCapCountRequest = null;
        	
        	try {    			
        		jsonData =  request.body();
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;        			
        		}
                initializeCapCountRequest = jsonMapper.readValue(jsonData, SparkInitializeCapCountRequest.class);
    			logger.debug("put /caphandlerservices/initializecap: " + initializeCapCountRequest);
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	 
        	try {
    			capCountService.initializeCapCountBackfill(initializeCapCountRequest);
                return REST_WS_OK_RESP;
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });     
                
        put (apiSparkWebServiceRootUri + apiSaveOnlyInitializeCapUri, (request, response) -> {
        	String jsonData;
        	SparkInitializeCapCountRequest initializeCapCountRequest = null;
        	
        	try {    			
        		jsonData =  request.body();
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;        			
        		}
                initializeCapCountRequest = jsonMapper.readValue(jsonData, SparkInitializeCapCountRequest.class);
    			logger.debug("put /caphandlerservices/initializecap: " + initializeCapCountRequest);
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	 
        	try {
        		capCountService.saveInitCapCountReq(initializeCapCountRequest);
                return REST_WS_OK_RESP;
        	} catch (Throwable exc) {
        		logger.error("", exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        });     
        
        // save increment cap request
        post (apiSparkWebServiceRootUri + incrementCapUri, (request, response)  -> { 
        	String jsonData = null;
        	SparkIncrementCapCountRequest sparkIncrementCapCountReq = null;
        	
        	try {
        		jsonData =  request.body();        		        				                		
        		if (jsonData == null || jsonData.trim().length() == 0) {
            		return REST_WS_NO_JSON_DATA_RESP;        			
        		}
        		sparkIncrementCapCountReq = jsonMapper.readValue(jsonData, SparkIncrementCapCountRequest.class);        		
        	} catch (Throwable exc) {
        		logger.error("jsonData: " + jsonData, exc);
        		return REST_WS_INVALID_JSON_DATA_RESP;
        	}
        	  
        	try {
                // To do save increment cap request
        		capCountService.incrementCapCount(sparkIncrementCapCountReq);
                return REST_WS_OK_RESP;
        	} catch (Throwable exc) {
        		logger.error("sparkIncrementCapCountReq: " + sparkIncrementCapCountReq, exc);
        		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        		return exc.getLocalizedMessage();
        	}
        }, jsonTransformer);
        
		logger.debug("Ready to receive requests at apiSparkWebServiceRootUri: " + apiSparkWebServiceRootUri);
    }
    
	public void incrementAndSaveInitializeCapCountByWS_Scheduled (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		String reqStr;
		HttpClient httpClient = null;
		HttpPut  httpRequest;
		StringEntity input;
		HttpResponse response;
		int respCode;
		BufferedReader respBufferReader;
				
		httpClient =  HttpClientBuilder.create().build();
		logger.debug("apiInitializeCapScheduledUrl: " + apiInitializeCapScheduledUrl);
		httpRequest = new HttpPut (apiInitializeCapScheduledUrl);
		reqStr = this.jsonMapper.writeValueAsString(initializeCapCountRequest);
		input = new StringEntity(reqStr,"UTF-8");
		input.setContentType("application/json");
		StringEntity entity = new StringEntity(reqStr, "UTF-8");
		httpRequest.setEntity(entity);
		response = httpClient.execute(httpRequest);

		respBufferReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		StringBuilder output = new StringBuilder ();
		String outputLine;
		while ((outputLine = respBufferReader.readLine()) != null) {
			output.append(outputLine);
		}
		logger.debug("Incr resp: " + output + " for Incr req: " + reqStr);

		respCode = response.getStatusLine().getStatusCode(); 
		if (respCode != 200) 
		{
			throw new RuntimeException("Failed : HTTP error code : " + respCode);
		}
	}	    	
    
	public void incrementAndSaveInitializeCapCountByWS_Backfill (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		String reqStr;
		HttpClient httpClient = null;
		HttpPut  httpRequest;
		StringEntity input;
		HttpResponse response;
		int respCode;
		BufferedReader respBufferReader;
				
		httpClient =  HttpClientBuilder.create().build();
		logger.debug("apiInitializeCapScheduledUrl: " + apiInitializeCapBackfillUrl);
		httpRequest = new HttpPut (apiInitializeCapBackfillUrl);
		reqStr = this.jsonMapper.writeValueAsString(initializeCapCountRequest);
		input = new StringEntity(reqStr,"UTF-8");
		input.setContentType("application/json");
		StringEntity entity = new StringEntity(reqStr, "UTF-8");
		httpRequest.setEntity(entity);
		response = httpClient.execute(httpRequest);

		respBufferReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		StringBuilder output = new StringBuilder ();
		String outputLine;
		while ((outputLine = respBufferReader.readLine()) != null) {
			output.append(outputLine);
		}
		logger.debug("Incr resp: " + output + " for Incr req: " + reqStr);

		respCode = response.getStatusLine().getStatusCode(); 
		if (respCode != 200) 
		{
			throw new RuntimeException("Failed : HTTP error code : " + respCode);
		}
	}	    	
    
	public void saveInitializeCapCountByWS (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		String reqStr;
		HttpClient httpClient = null;
		HttpPut  httpRequest;
		StringEntity input;
		HttpResponse response;
		int respCode;
		BufferedReader respBufferReader;
				
		httpClient =  HttpClientBuilder.create().build();
		logger.debug("apiSaveOnlyInitializeCapUrl: " + apiSaveOnlyInitializeCapUrl);
		httpRequest = new HttpPut (apiSaveOnlyInitializeCapUrl);
		reqStr = this.jsonMapper.writeValueAsString(initializeCapCountRequest);
		input = new StringEntity(reqStr,"UTF-8");
		input.setContentType("application/json");
		StringEntity entity = new StringEntity(reqStr, "UTF-8");
		httpRequest.setEntity(entity);
		response = httpClient.execute(httpRequest);

		respBufferReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		StringBuilder output = new StringBuilder ();
		String outputLine;
		while ((outputLine = respBufferReader.readLine()) != null) {
			output.append(outputLine);
		}
		logger.debug("Incr resp: " + output + " for Incr req: " + reqStr);

		respCode = response.getStatusLine().getStatusCode(); 
		if (respCode != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + respCode);
		}
	}	    	
    
	public void incrementCapCountByWS (SparkIncrementCapCountRequest sparkIncrementCapCountReq) throws Throwable {
		String reqStr;
		HttpClient httpClient = null;
		HttpPost  httpRequest;
		StringEntity input;
		HttpResponse response;
		int respCode;
		BufferedReader respBufferReader;
		
		ClassLoader classLoader = this.getClass().getClassLoader();
		URL resource = classLoader.getResource("org/apache/http/impl/client/HttpClientBuilder.class");
		logger.debug("HttpClientBuilder resource " + resource.toString());
		
		httpClient =  HttpClientBuilder.create().build();
		logger.debug("incrementCapUrl: " + incrementCapUrl);
		httpRequest = new HttpPost (incrementCapUrl);
		reqStr = this.jsonMapper.writeValueAsString(sparkIncrementCapCountReq);
		input = new StringEntity(reqStr,"UTF-8");
		input.setContentType("application/json");
		StringEntity entity = new StringEntity(reqStr, "UTF-8");
		httpRequest.setEntity(entity);
		response = httpClient.execute(httpRequest);

		respBufferReader = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

		StringBuilder output = new StringBuilder ();
		String outputLine;
		while ((outputLine = respBufferReader.readLine()) != null) {
			output.append(outputLine);
		}
		logger.debug("Incr resp: " + output + " for Incr req: " + reqStr);

		respCode = response.getStatusLine().getStatusCode(); 
		if (respCode != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + respCode);
		}
	}
	
	public void incrementCapCountByWS_Delete (SparkIncrementCapCountRequest sparkIncrementCapCountReq) throws Throwable {
		String reqStr;
		DefaultHttpClient httpClient = null;
		HttpPost httpRequest;
		
		try {
			httpClient = new DefaultHttpClient();
			httpRequest = new HttpPost(apiSparkWebServicesUri);
			reqStr = this.jsonMapper.writeValueAsString(sparkIncrementCapCountReq);
			
			StringEntity input = new StringEntity(reqStr);
			input.setContentType("application/json");
			httpRequest.setEntity(input);

			HttpResponse response = httpClient.execute(httpRequest);

			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			StringBuilder output = new StringBuilder ();
			String outputLine;
			while ((outputLine = br.readLine()) != null) {
				output.append(outputLine);
			}
			logger.debug("Incr resp: " + output + " for Incr req: " + reqStr);

			if (response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatusLine().getStatusCode());
			}
			
		} finally {
			if (httpClient != null) {
				httpClient.close();				
			}
		}
	}
	
    
}
