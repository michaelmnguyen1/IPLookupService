package com.getcake.capcount.services;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import com.datastax.driver.core.Session;
import com.getcake.capcount.dao.CapCountDao;
import com.getcake.capcount.dao.MsSqlCapCountDao;
import com.getcake.capcount.model.*;

import org.apache.spark.streaming.Time;

import com.typesafe.config.Config; 

import org.apache.log4j.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

@SuppressWarnings("serial")
public class CapCountService  implements Serializable {

	private static final Logger logger = Logger.getLogger(CapCountService.class);
	private static String region;
	private static int backFillTimeOffsetInMilliSeconds, retryTimeOffsetInMilliSeconds;
	
	private static CapCountService instance;
	
	private CapCountWebService capCountWebService;
	private CapCountDao capCountDao;
	private long statusCount;
	private Iso8601JsonMapper jsonMapper;
	private TimerPoolShared timerPoolShared;
	
	private Properties properties;
	private Config topLevelClientsConfig;
	
	private String initCapSparkInitializeCapUrl;
	
	public static int getBackFillTimeOffsetInMilliSeconds () {
		return backFillTimeOffsetInMilliSeconds;
	}
	
	public static int getRetryTimeOffsetInMilliSeconds () {
		return retryTimeOffsetInMilliSeconds;
	}

	public static CapCountService getInstance(Properties properties, Config topLevelClientsConfig, boolean initDaoFlag,
		CapCountWebService capCountWebService) {
		getInstance(properties, topLevelClientsConfig, initDaoFlag) ;
		if (instance.capCountWebService == null) {
			instance.capCountWebService = capCountWebService;			
		}
		return instance;
	}

	public static CapCountService getInstance(Properties properties, Config topLevelClientsConfig, boolean initDaoFlag) {
		if (instance != null) {
			logger.debug("CapCountService - getInstance instance not null on first check " +
					" - thread id " + Thread.currentThread().getId() +
					" instance: " + instance);			
			return instance;
		}

		synchronized (CapCountService.class) {
			
			logger.debug("CapCountService - getInstance in sync " +
				" - thread id " + Thread.currentThread().getId());
			if (instance != null) {
				logger.debug("CapCountService - getInstance instance in sync block not null on second check " +
						" - thread id " + Thread.currentThread().getId() +
						" instance: " + instance);
				return instance;
			}
			
			logger.debug("CapCountService - getInstance instance in sync block before create new instance " + 
					" - thread id " + Thread.currentThread().getId());
			instance = new CapCountService(properties, topLevelClientsConfig, initDaoFlag);			
			logger.debug("CapCountService - getInstance instance in sync block done init new instance " + 
					" - thread id " + Thread.currentThread().getId());
			return instance;
		}
	}	

	private CapCountService (Properties properties, Config topLevelClientsConfig, boolean initDaoFlag) {
		jsonMapper = Iso8601JsonMapper.getInstance();
        init(properties, topLevelClientsConfig, initDaoFlag);
	}

	private void init (Properties properties, Config topLevelClientsConfig, boolean initCassandraConnFlag) {
		
		this.properties = properties;
		this.topLevelClientsConfig = topLevelClientsConfig;
		
		backFillTimeOffsetInMilliSeconds = Integer.parseInt(properties.getProperty("backFillTimeOffsetInSeconds")) * 1000;
		backFillTimeOffsetInMilliSeconds = Integer.parseInt(properties.getProperty("backFillTimeOffsetInSeconds")) * 1000;
		retryTimeOffsetInMilliSeconds = Integer.parseInt(properties.getProperty("retryTimeOffsetInSeconds")) * 1000;		
		
		region = properties.getProperty("region");
		
		logger.debug("CapCountService - init calling CapCountDao.getInstance " +
				" - thread id " + Thread.currentThread().getId());
		capCountDao = CapCountDao.getInstance(properties, topLevelClientsConfig, initCassandraConnFlag);
		logger.debug("CapCountService - init - created capCountDao: " + 
			" - thread id " + Thread.currentThread().getId() + 
			" - capCountDao: " + capCountDao);			
		
		this.timerPoolShared = TimerPoolShared.init(Integer.parseInt(properties.getProperty("timerPoolSize")));
		
		initCapSparkInitializeCapUrl = properties.getProperty("initCapSparkWebServer");
		initCapSparkInitializeCapUrl += ":";
		initCapSparkInitializeCapUrl += properties.getProperty("initCapSparkWebPort");
		initCapSparkInitializeCapUrl += properties.getProperty("initCapSparkWebServiceRootUri");
		initCapSparkInitializeCapUrl += properties.getProperty("initCapSparkInitializeCapUri");
				
	}

	public Iso8601JsonMapper getJsonMapper () {
		return jsonMapper;
	}
	
	public String getStatus () {
		return "OK";
	}
	
    public long getStatusCount() {    	
    	return ++statusCount;
    }
	
	public void initializeCapCountFirstSteps_Spark (SparkInitializeCapCountRequest initializeCapCountRequest,
		int runCount, long callerThreadId) throws Throwable {			
		CakeTimer cakeTimer = null;
		Date scheduledDate;
		
		initializeCapCountRequest.setRequestUuid(initializeCapCountRequest.getRequestUuid() + "-" + runCount);
		
		scheduledDate = initializeCapCountRequest.getEffectiveDate();
		cakeTimer = getAvailCakeTimer(initializeCapCountRequest.getEffectiveDate().getTime());
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled);
		scheduleInitReqTimerTask(cakeTimer, scheduledDate, initializeCapCountRequest, runCount, callerThreadId);		
		logger.debug("testTimer scheduled " + scheduledDate + " for " + 
			initializeCapCountRequest.toString() );		
	}

	public CakeTimer getAvailCakeTimer (long scheduledTime) {
		CakeTimer cakeTimer = null;
		
		cakeTimer = timerPoolShared.getTimer(scheduledTime);
		return cakeTimer;
	}		
	
	public InitCapCountTimerTask_Spark scheduleInitReqTimerTask (CakeTimer cakeTimer, Date scheduledDate,  
		SparkInitializeCapCountRequest initializeCapCountRequest, int runCount, long callerThreadId) {
		InitCapCountTimerTask_Spark initCapCountTimerTask_Spark;
		ConcurrentMap<Long, CakeTimerTask> scheduledTaskMap;

		Function<Long, InitCapCountTimerTask_Spark> createNewTask = new Function<Long, InitCapCountTimerTask_Spark> () {

			@Override
			public InitCapCountTimerTask_Spark apply(Long scheduledTime) {
				return new InitCapCountTimerTask_Spark (properties, topLevelClientsConfig, 
						cakeTimer, initializeCapCountRequest, scheduledDate, runCount, callerThreadId);
			}			
		};
		
		scheduledTaskMap = cakeTimer.getScheduledTaskMap();		
		
		initCapCountTimerTask_Spark = (InitCapCountTimerTask_Spark) scheduledTaskMap.computeIfAbsent(scheduledDate.getTime(), createNewTask );
		initCapCountTimerTask_Spark.addInitializeCapCountRequest(initializeCapCountRequest);
		cakeTimer.scheduleSharedTask(initCapCountTimerTask_Spark, scheduledDate);
		return initCapCountTimerTask_Spark;
	}
	
	public void initializeCapCountFirstSteps (SparkInitializeCapCountRequest initializeCapCountRequest, String requestReceivedRegion) throws Throwable {			
		String batchtUuid;
	    int clientId;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
	    KeySpaceInfo keySpaceInfo;
	    CapEntityDetails capEntityCountInfo;
		
	    clientId = initializeCapCountRequest.getClientId();
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
		
		capEntityCountInfo = this.getCapEntityDetails(initializeCapCountRequest);		
			
		if (System.currentTimeMillis() >= (initializeCapCountRequest.getEffectiveDate().getTime())) {			
			Time batchTime = new Time (initializeCapCountRequest.getRequestDate().getTime());
			batchtUuid = "Init Req-Client Id:" +  initializeCapCountRequest.getClientId() + " - Effective Date:" + initializeCapCountRequest.getEffectiveDate() +
				" - Request Date:" + initializeCapCountRequest.getRequestDate() + " - increment count:" + initializeCapCountRequest.getIncrementCount();	
			initializeCapCountRequest.setBatchUuid(batchtUuid);
			initializeCapCountRequest.setBatchTime(batchTime);
			incrementCapCount(initializeCapCountRequest);
			
			initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Increment_Done_Skipped_Scheduling);
			capCountDao.saveInitCapCountReq(keySpaceInfo, clientDaoCapKeyInfo, capEntityCountInfo, initializeCapCountRequest, 
				requestReceivedRegion, region, initializeCapCountRequest.getInitializeRequestStatus(), initializeCapCountRequest.getNumRetries ());
			return;
		}
						
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Ready);
		capCountDao.saveInitCapCountReq(keySpaceInfo, clientDaoCapKeyInfo, capEntityCountInfo, initializeCapCountRequest, 
			requestReceivedRegion, region, initializeCapCountRequest.getInitializeRequestStatus(), initializeCapCountRequest.getNumRetries ());
		notifyInitializeCapCountByWS (initializeCapCountRequest);
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled);
		capCountDao.saveInitCapCountReq(keySpaceInfo, clientDaoCapKeyInfo, capEntityCountInfo, initializeCapCountRequest, 
			requestReceivedRegion, region, initializeCapCountRequest.getInitializeRequestStatus(), initializeCapCountRequest.getNumRetries ());		
	}
	
	public void notifyInitializeCapCountByWS (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		String reqStr;
		HttpClient httpClient = null;
		HttpPost  httpRequest;
		StringEntity input;
		HttpResponse response;  
		int respCode;
		BufferedReader respBufferReader;
		
		httpClient =  HttpClientBuilder.create().build();
		logger.debug("initCapSparkInitializeCapUrl: " + initCapSparkInitializeCapUrl);
		httpRequest = new HttpPost (initCapSparkInitializeCapUrl);
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

	public void initializeCapCountScheduled_Spark (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		this.capCountWebService.incrementAndSaveInitializeCapCountByWS_Scheduled(initializeCapCountRequest); 					
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled_Done);
	}
	
	public void initializeCapCountScheduled (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
	    int clientId;

	    KeySpaceInfo keySpaceInfo;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
	    CapEntityDetails capEntityDetails;
	    MsSqlCapCountDao sqlServerDBInfo; 
	    ClickEventCounts capEntityCounts;
	    CapEntity capEntity;
	    ClickEvent clickEvent;
	    
	    clientId = initializeCapCountRequest.getClientId();
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId, false);
		
		sqlServerDBInfo = clientDaoCapKeyInfo.getPrimarySqlServer();
		capEntityDetails = this.getCapEntityDetails(initializeCapCountRequest);
		initializeCapCountRequest.setIncrementCount(sqlServerDBInfo.getClickEventCount(clientDaoCapKeyInfo, capEntityDetails));
		
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
    	capEntity = capEntityDetails.getCapEntity();
    	clickEvent = capEntityDetails.getClickEvent();		

		switch (capEntityDetails.getCountType()) {
		case CLICK:			
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getClickCount());				
			}
			break;

		case MACRO_EVENT:
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getMacroEventCount());				
			}
			break;
			
		case GLOBAL:
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getGlobalEventCount());				
			}
			break;
			
		default: 
			CapEntityMicroEvents capEntityMicroEvents;
			capEntityMicroEvents = new CapEntityMicroEvents ();
			List<MicroEvent> eventReqInfoList = new ArrayList<>();
			eventReqInfoList.add(capEntity.getMicroEvent());
			capEntityMicroEvents.setMicroEvents(eventReqInfoList);
			capEntityMicroEvents.setId(capEntity.getId());
			capEntityMicroEvents.setCapEntityType(capEntity.getCapEntityType());
			capCountDao.getMicroEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntityMicroEvents);
			MicroEvent microevent = capEntityMicroEvents.getMicroEvents().get(0);
			if (microevent.getCount() != 0) {
				initializeCapCountRequest.setIncrementCount( - microevent.getCount());								
			}
			break;			
		}			
		
		InitializeRequestStatus prevInitializeRequestStatus = initializeCapCountRequest.getInitializeRequestStatus(); 
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled_Done);
		try {
    		incrementCapCount(initializeCapCountRequest);
    		saveInitCapCountReq(initializeCapCountRequest);
			
		} catch (Throwable exc) {
			initializeCapCountRequest.setInitializeRequestStatus(prevInitializeRequestStatus);
			throw exc;
		}		
	}
	
	public void saveInitCapCountReq (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		String requestReceivedRegion;
	    int clientId;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
	    KeySpaceInfo keySpaceInfo;
	    CapEntityDetails capEntityCountInfo;
				
	    requestReceivedRegion = initializeCapCountRequest.getRequestReceivedRegion();
	    clientId = initializeCapCountRequest.getClientId();
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
		capEntityCountInfo = this.getCapEntityDetails(initializeCapCountRequest);
		
		capCountDao.saveInitCapCountReq(keySpaceInfo, clientDaoCapKeyInfo, capEntityCountInfo, initializeCapCountRequest, requestReceivedRegion, 
			region, initializeCapCountRequest.getInitializeRequestStatus(), initializeCapCountRequest.getNumRetries());				
	}
	
	public void initializeCapCountBackfill_Spark (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
		this.capCountWebService.incrementAndSaveInitializeCapCountByWS_Backfill(initializeCapCountRequest); 							
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Backfill_Done);
	}
	
	public void initializeCapCountBackfill (SparkInitializeCapCountRequest initializeCapCountRequest) throws Throwable {
	    int clientId;
	    KeySpaceInfo keySpaceInfo;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
	    CapEntityDetails capEntityDetails;
	    MsSqlCapCountDao sqlServerDBInfo; 
	    ClickEventCounts capEntityCounts;
	    CapEntity capEntity;
	    ClickEvent clickEvent;
	    
	    clientId = initializeCapCountRequest.getClientId();
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId, false);
		
		sqlServerDBInfo = clientDaoCapKeyInfo.getPrimarySqlServer();
		capEntityDetails = this.getCapEntityDetails(initializeCapCountRequest);
		initializeCapCountRequest.setIncrementCount(sqlServerDBInfo.getClickEventCount(clientDaoCapKeyInfo, capEntityDetails));
		
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
    	capEntity = capEntityDetails.getCapEntity();
    	clickEvent = capEntityDetails.getClickEvent(); 

		switch (capEntityDetails.getCountType()) {
		case CLICK:			
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getClickCount());				
			}
			break;

		case MACRO_EVENT:
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getMacroEventCount());				
			}
			break;
			
		case GLOBAL:
			capEntityCounts = capCountDao.getClickEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntity, clickEvent);
			if (capEntityCounts != null) {
				initializeCapCountRequest.setIncrementCount( - capEntityCounts.getGlobalEventCount());				
			}
			break;
			
		default: 
			CapEntityMicroEvents capEntityMicroEvents;
			capEntityMicroEvents = new CapEntityMicroEvents ();
			List<MicroEvent> eventReqInfoList = new ArrayList<>();
			eventReqInfoList.add(capEntity.getMicroEvent());
			capEntityMicroEvents.setMicroEvents(eventReqInfoList);
			capEntityMicroEvents.setId(capEntity.getId());
			capEntityMicroEvents.setCapEntityType(capEntity.getCapEntityType());
			capCountDao.getMicroEventCounts(clientDaoCapKeyInfo, keySpaceInfo.getCountKeySpaceSession(), capEntityMicroEvents);
			MicroEvent microevent = capEntityMicroEvents.getMicroEvents().get(0);
			if (microevent.getCount() != 0) {
				initializeCapCountRequest.setIncrementCount( - microevent.getCount());								
			}
			break;			
		}			
		
		InitializeRequestStatus prevInitializeRequestStatus = initializeCapCountRequest.getInitializeRequestStatus(); 
		initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Backfill_Done);
		try {
    		incrementCapCount(initializeCapCountRequest);
    		// to do: save as part of increment
    		saveInitCapCountReq(initializeCapCountRequest);
			
		} catch (Throwable exc) {
			initializeCapCountRequest.setInitializeRequestStatus(prevInitializeRequestStatus);
			throw exc;
		}		
	}
		
	public ReadCapCountRequest getCapCounts (ReadCapCountRequest capCountRequest) {
	    int clientId;
	    KeySpaceInfo keySpaceInfo;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
		Session countKeySpaceSession;
		
        clientId = capCountRequest.getClientId();
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
		countKeySpaceSession = keySpaceInfo.getCountKeySpaceSession();
		
		this.getCapCounts(keySpaceInfo, clientDaoCapKeyInfo, countKeySpaceSession, capCountRequest.getOffer());
		this.getCapCounts(keySpaceInfo, clientDaoCapKeyInfo, countKeySpaceSession, capCountRequest.getOfferContract());
		this.getCapCounts(keySpaceInfo, clientDaoCapKeyInfo, countKeySpaceSession, capCountRequest.getCampaign());
		
		return capCountRequest;
	}
	
	private void getCapCounts (KeySpaceInfo keySpaceInfo,  ClientDaoCapKeyInfo clientDaoCapKeyInfo, Session countKeySpaceSession,
		CapEntityMicroEvents capEntityMicroEvents) {
		ClickEventCounts capEntityCounts = null;
		ClickEvent clickEvent;
		Map<ClickEvent, ClickEventCounts> capEntityCountsMap;

		if (capEntityMicroEvents == null) {
			return;			
		}
				
		capEntityCountsMap = new HashMap<>();
		clickEvent = capEntityMicroEvents.getClick();
		if (clickEvent != null) {
			capEntityCounts = this.capCountDao.getClickEventCounts(clientDaoCapKeyInfo, countKeySpaceSession, capEntityMicroEvents, clickEvent);
			clickEvent.setCount(capEntityCounts.getClickCount());				
			capEntityCountsMap.put(clickEvent, capEntityCounts);
		}			
		
		clickEvent = capEntityMicroEvents.getMacroEvent();
		if (clickEvent != null) {
			capEntityCounts = capEntityCountsMap.get(clickEvent);
			if (capEntityCounts == null) {
				capEntityCounts = this.capCountDao.getClickEventCounts(clientDaoCapKeyInfo, countKeySpaceSession, capEntityMicroEvents, clickEvent);
				capEntityCountsMap.put(clickEvent, capEntityCounts);
			}
			clickEvent.setCount(capEntityCounts.getMacroEventCount());
		}			
		
		clickEvent = capEntityMicroEvents.getGlobalEvent();
		if (clickEvent != null) {
			capEntityCounts = capEntityCountsMap.get(clickEvent);
			if (capEntityCounts == null) {
				capEntityCounts = this.capCountDao.getClickEventCounts(clientDaoCapKeyInfo, countKeySpaceSession, capEntityMicroEvents, clickEvent);
			}
			clickEvent.setCount(capEntityCounts.getGlobalEventCount());
		}			

		capCountDao.getMicroEventCounts(clientDaoCapKeyInfo, countKeySpaceSession, capEntityMicroEvents);		
	}
	
	public void incrementCapCount (SparkIncrementCapCountRequest sparkIncrementCapCountReq) throws Throwable {
	    int clientId;
	    KeySpaceInfo keySpaceInfo;
	    ClientDaoCapKeyInfo clientDaoCapKeyInfo = null;
	    CapEntityDetails capEntityDetails;
	    CapEntity capEntity;
		PendingInitReqInfo pendingInitReqInfo;
		DBTransactionInfo dbTransactionInfo;

		String requestUuids [] = new String [sparkIncrementCapCountReq.getMergedRequestUuids().size()];
		requestUuids = sparkIncrementCapCountReq.getMergedRequestUuids().toArray(requestUuids);
		String requestUuidsStr = Arrays.toString(requestUuids);
  
        clientId = sparkIncrementCapCountReq.getClientId();
	    keySpaceInfo = capCountDao.getClientKeySpaceInfo(clientId);
		clientDaoCapKeyInfo = capCountDao.getClientCapKeyDaoInfo(clientId);
		capEntityDetails = this.getCapEntityDetails(sparkIncrementCapCountReq);
		
		pendingInitReqInfo = capCountDao.getInitCapEntityCount(keySpaceInfo, clientDaoCapKeyInfo, capEntityDetails);
		if (pendingInitReqInfo != null) {
	    	capEntity = capEntityDetails.getCapEntity();
	    	logger.debug("pending initialize request, so skip increemnt requesr for " + capEntity.getCapEntityType() + " - " + capEntity.getId() + 
       	    	" countType: " + capEntityDetails.getCountType() + 
   	    		" startDate: " + capEntityDetails.getClickEvent().getStartDate() + 
				" endDate: " + capEntityDetails.getClickEvent().getEndDate() + 
				" endDate: " + capEntityDetails.getCapEntity());     				
	    	return;
		}		
		dbTransactionInfo = capCountDao.getDBTransactionInfo();
		capCountDao.processIncrementCapCount(keySpaceInfo, clientDaoCapKeyInfo, capEntityDetails, sparkIncrementCapCountReq, dbTransactionInfo);
	}

	public CapEntityDetails getReadCapEntityDetails (ReadCapCountRequest capCountRequest) {
		CapEntityDetails capEntityCountInfo;
		CapEntity capEntity;
		ClickEvent clickEvent;
		MicroEvent eventReqInfo;
		
		capEntity = capCountRequest.getOfferContract();								
		if (capEntity == null) {
    		capEntity = capCountRequest.getOffer();
    		if (capEntity == null) {
        		capEntity = capCountRequest.getCampaign();								    			
        		if (capEntity == null) {
            		throw new RuntimeException ("No capEntity specified for Cap Count Request " + capCountRequest);								    			
        		}
    		}
		}
		capEntityCountInfo = new CapEntityDetails ();
		capEntityCountInfo.setCapEntity(capEntity);
		
		clickEvent = capEntity.getClick();
		if (clickEvent != null) {
			capEntityCountInfo.setClickEvent(clickEvent);
			capEntityCountInfo.setCountType(CountType.CLICK);
			return capEntityCountInfo;
		}
		
		clickEvent = capEntity.getMacroEvent();
		if (clickEvent != null) {
			capEntityCountInfo.setClickEvent(clickEvent);
			capEntityCountInfo.setCountType(CountType.MACRO_EVENT);
			return capEntityCountInfo;
		}
		
		clickEvent = capEntity.getGlobalEvent();
		if (clickEvent != null) {
			capEntityCountInfo.setClickEvent(clickEvent);
			capEntityCountInfo.setCountType(CountType.GLOBAL);
			return capEntityCountInfo;
		}
		
		eventReqInfo = capEntity.getMicroEvent();
		if (eventReqInfo != null) {
			capEntityCountInfo.setClickEvent(clickEvent);
			capEntityCountInfo.setCountType(CountType.MICRO_EVENT);
			return capEntityCountInfo;
		}
		
		throw new RuntimeException ("No date range specified for capEntity " + capEntity);		
	}
		
	public CapEntityDetails getCapEntityDetails (IncrementCapCountRequest capCountRequest) {
		CapEntityDetails capEntityDetails;
		CapEntity capEntity;
		ClickEvent clickEvent;
		MicroEvent microEvent;
		
		capEntity = capCountRequest.getOfferContract();								
		if (capEntity == null) {
    		capEntity = capCountRequest.getOffer();
    		if (capEntity == null) {
        		capEntity = capCountRequest.getCampaign();								    			
        		if (capEntity == null) {
            		capEntity = capCountRequest.getGlobalEventReqInfo();								    			
            		if (capEntity == null) {
                		throw new RuntimeException ("No capEntity specified for Cap Count Request " + capCountRequest);								    			
            		}
        		}
    		}
		}
		capEntityDetails = new CapEntityDetails ();
		capEntityDetails.setCapEntity(capEntity);
		
		clickEvent = capEntity.getClick();
		if (clickEvent != null) {
			capEntityDetails.setClickEvent(clickEvent);
			capEntityDetails.setCountType(CountType.CLICK);
			return capEntityDetails;
		}
		
		clickEvent = capEntity.getMacroEvent();
		if (clickEvent != null) {
			capEntityDetails.setClickEvent(clickEvent);
			capEntityDetails.setCountType(CountType.MACRO_EVENT);
			return capEntityDetails;
		}
		
		clickEvent = capEntity.getGlobalEvent();
		if (clickEvent != null) {
			capEntityDetails.setClickEvent(clickEvent);
			capEntityDetails.setCountType(CountType.GLOBAL);
			return capEntityDetails;
		}
		
		microEvent = capEntity.getMicroEvent();
		if (microEvent != null) {
			capEntityDetails.setClickEvent(microEvent);
			capEntityDetails.setCountType(CountType.MICRO_EVENT);
			return capEntityDetails;
		}		
		throw new RuntimeException ("No date range specified for capEntity " + capEntity);		
	}	
}
