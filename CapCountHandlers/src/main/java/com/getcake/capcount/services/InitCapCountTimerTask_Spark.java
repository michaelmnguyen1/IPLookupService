package com.getcake.capcount.services;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;

import scala.Tuple2;

import com.getcake.capcount.model.CakeTimer;
import com.getcake.capcount.model.CakeTimerTask;
import com.getcake.capcount.model.InitializeRequestStatus;
import com.getcake.capcount.model.SparkInitializeCapCountRequest;
import com.typesafe.config.Config;

public class InitCapCountTimerTask_Spark extends CakeTimerTask implements Serializable {

	private static final Logger logger = Logger.getLogger(InitCapCountTimerTask_Spark.class);
	
	private static final int MAX_INIT_CAP_RETRIES = 3;
	
	private List<SparkInitializeCapCountRequest> initializeCapCountRequests;
	private int iterCount;  
	private int ws_runCount;
	private long callerThreadId, timerThreadId;
	
	private Properties properties;
	private Config topLevelClientsConfig;
	
	public InitCapCountTimerTask_Spark () {
	}
	
	public InitCapCountTimerTask_Spark (Properties properties, Config topLevelClientsConfig,
		CakeTimer cakeTimer, SparkInitializeCapCountRequest initializeCapCountRequest, Date scheduledTime,
		int ws_runCount, long callerThreadId) {
		
		this.properties = properties; 
		this.topLevelClientsConfig = topLevelClientsConfig; 
		this.ws_runCount = ws_runCount;
		this.callerThreadId = callerThreadId;
		
		initializeCapCountRequests = new ArrayList<> (1);
		initializeCapCountRequests.add(initializeCapCountRequest);
		this.cakeTimer = cakeTimer;
		// cakeTimer.schedule(this, scheduledTime);		
	}
	 
	public void addInitializeCapCountRequest (SparkInitializeCapCountRequest initializeCapCountRequest) {
		if (! initializeCapCountRequests.contains(initializeCapCountRequest)) {
			initializeCapCountRequests.add(initializeCapCountRequest);					
		}
	}
	
	public void run () {		
		runInitCapCountRequest ();
	}
	
    VoidFunction<Iterator <SparkInitializeCapCountRequest>> processInitCapCountRequestByPartitions = 
    	new VoidFunction<Iterator <SparkInitializeCapCountRequest>> () {

		private static final long serialVersionUID = 1L;

		@Override
		public void call(Iterator<SparkInitializeCapCountRequest> initializeCapCountRequests)
				throws Exception {
			Date currTime, scheduleTime;
			int partitionId = -1;
			SparkInitializeCapCountRequest initializeCapCountRequest = null;
			CapCountWebService capCountWebService;
			CapCountService capCountService;			
			
			try {				
				partitionId = TaskContext.getPartitionId();
				if (! initializeCapCountRequests.hasNext()) {
					// logger.debug("no data passed in for partitionId: " + partitionId);
					return;
				}					
				
				capCountWebService = CapCountWebService.getInstance(properties, topLevelClientsConfig);				
				capCountService = CapCountService.getInstance(properties, topLevelClientsConfig, false, capCountWebService);				
				currTime = Calendar.getInstance().getTime();								
				scheduleTime = Calendar.getInstance().getTime();
				scheduleTime.setTime(scheduledExecutionTime());
				while (initializeCapCountRequests.hasNext()) {
					initializeCapCountRequest = initializeCapCountRequests.next();
					
					try {
						initializeCapCountRequest.setBatchTime(new Time (currTime.getTime()));
						initializeCapCountRequest.setPartitionId(partitionId);
						initializeCapCountRequest.setBatchUuid("partition id: " + partitionId + 
							" - batch time: " + initializeCapCountRequest.getBatchTime());						
						switch (initializeCapCountRequest.getInitializeRequestStatus()) {
						case Scheduled:
						case Scheduled_Retry:
							logger.debug("Scheduled for " + initializeCapCountRequest);
							capCountService.initializeCapCountScheduled_Spark(initializeCapCountRequest);
							break;

						case Backfill:
						case Backfill_Retry:
							capCountService.initializeCapCountBackfill_Spark(initializeCapCountRequest);				
							break;
							
						default:
							throw new Exception ("Invalid InitializeCapCountRequest Status: " + initializeCapCountRequest.getInitializeRequestStatus());
						}						
					} catch (Throwable exc) {
						try {
							logger.error("", exc);		
							if (initializeCapCountRequest == null) {
								logger.error("initializeCapCountRequest is null");
								break;
							}
							switch (initializeCapCountRequest.getInitializeRequestStatus()) {
							case Scheduled:
							case Scheduled_Retry:
								initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled_Failed);
								break;
								
							case Backfill:
							case Backfill_Retry:
								initializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Backfill_Failed);
								break;
								
							default:
							}
							
							capCountWebService.saveInitializeCapCountByWS(initializeCapCountRequest);
							
						} catch (Throwable exc2) {
						     logger.error("", exc2);
						}						
					}					
				}
				
			} catch (Throwable exc) {
				logger.error("processInitCapCountRequestByPartitions Error partition id: " + partitionId + " - threadId: " + Thread.currentThread().getId() +  " - req: " + initializeCapCountRequest, exc);
				throw new Exception (exc);
			}
		}    	
    };
    		
    public InitCapCountTimerTask_Spark getThisObject () {
    	return this;
    }
    
	private JavaRDD<SparkInitializeCapCountRequest> processInitCapCountRequestsByDriver () {
		JavaRDD<SparkInitializeCapCountRequest> initReqRDD =
				CapCountInitializeRequestHandler.javaSparkContext.parallelize(initializeCapCountRequests);
		JavaRDD<SparkInitializeCapCountRequest> cachedInitReqRDD;
		
		cachedInitReqRDD = initReqRDD.cache();
		cachedInitReqRDD.foreachPartition(processInitCapCountRequestByPartitions);
		
		return cachedInitReqRDD;
	}
	
	public void runInitCapCountRequest () {
		String msg = null;
		Date scheduledDate;		
		InitCapCountTimerTask_Spark initCapCountTimerTask_Spark = null;
		JavaRDD<SparkInitializeCapCountRequest> cachedInitReqRDD;
		List<SparkInitializeCapCountRequest> processedInitializeCapCountRequests;
		CapCountService capCountService;
		CapCountWebService capCountWebService;
		
    	try {
			
    		timerThreadId = Thread.currentThread().getId();
    		cachedInitReqRDD = processInitCapCountRequestsByDriver ();
    		processedInitializeCapCountRequests = cachedInitReqRDD.collect();			
    		capCountService = CapCountService.getInstance(properties, topLevelClientsConfig, false);
    		capCountWebService = CapCountWebService.getInstance(properties, topLevelClientsConfig);				    		
    		initializeCapCountRequests.clear();
    		
			for (SparkInitializeCapCountRequest processedInitializeCapCountRequest : processedInitializeCapCountRequests) {
				msg = "runInitCapCountRequest requestUuid: " + processedInitializeCapCountRequest.getRequestUuid() +
						" - thread id: " + Thread.currentThread().getId() +
						" - RequestStatus: " + processedInitializeCapCountRequest.getInitializeRequestStatus() +
						" - ws_runCount: " + ws_runCount +
						" - callerThreadId: " + callerThreadId + " - timerThreadId:"  + timerThreadId + 
						" - batchUuid: " + processedInitializeCapCountRequest.getBatchUuid() +
						" - iterationCount: " + iterCount;
	    		logger.debug("start " + msg);
			    
				switch (processedInitializeCapCountRequest.getInitializeRequestStatus()) {
				case Scheduled_Done:
					logger.debug("Scheduled_Done for " + processedInitializeCapCountRequest);
					processedInitializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Backfill);
					capCountWebService.saveInitializeCapCountByWS(processedInitializeCapCountRequest);					
					if (initCapCountTimerTask_Spark == null) {
					    scheduledDate = Calendar.getInstance().getTime(); 
						scheduledDate.setTime(processedInitializeCapCountRequest.getEffectiveDate().getTime() + CapCountService.getBackFillTimeOffsetInMilliSeconds());
						cakeTimer = capCountService.getAvailCakeTimer (scheduledDate.getTime());
						initCapCountTimerTask_Spark = capCountService.scheduleInitReqTimerTask(cakeTimer, scheduledDate, 
							processedInitializeCapCountRequest, ws_runCount, callerThreadId);						
					} else {
						initCapCountTimerTask_Spark.addInitializeCapCountRequest(processedInitializeCapCountRequest);						
					}					
					break;
					
				case Scheduled_Retry:
					// continue
				case Scheduled:
				case Scheduled_Failed:
					logger.debug("Scheduled failed with current status: " + processedInitializeCapCountRequest.getInitializeRequestStatus() + 
						" - " + processedInitializeCapCountRequest);
					if (processedInitializeCapCountRequest.getNumRetries() >= MAX_INIT_CAP_RETRIES) {
						handleMaxRetriesReached (processedInitializeCapCountRequest, InitializeRequestStatus.Scheduled_Failed_Max_Retries_Exceeded,
							cachedInitReqRDD, capCountWebService);
						break;
					}
					
					processedInitializeCapCountRequest.incrementNumRetries ();
					processedInitializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Scheduled_Retry);
					if (initCapCountTimerTask_Spark == null) {
					    scheduledDate = Calendar.getInstance().getTime(); 
						scheduledDate.setTime(processedInitializeCapCountRequest.getEffectiveDate().getTime() + CapCountService.getBackFillTimeOffsetInMilliSeconds());
						cakeTimer = capCountService.getAvailCakeTimer (scheduledDate.getTime());
						initCapCountTimerTask_Spark = capCountService.scheduleInitReqTimerTask(cakeTimer, scheduledDate, 
							processedInitializeCapCountRequest, ws_runCount, callerThreadId);
					} else {
						initCapCountTimerTask_Spark.addInitializeCapCountRequest(processedInitializeCapCountRequest);
					}					
					break;

				case Backfill:
				case Backfill_Retry:
				case Backfill_Failed:
					logger.debug("Scheduled failed with current status: " + processedInitializeCapCountRequest.getInitializeRequestStatus() + 
							" - " + processedInitializeCapCountRequest);
					if (processedInitializeCapCountRequest.getNumRetries() >= MAX_INIT_CAP_RETRIES) {
						handleMaxRetriesReached (processedInitializeCapCountRequest, InitializeRequestStatus.Backfill_Retry_Max_Retries_Exceeded,
							cachedInitReqRDD, capCountWebService);
						break;
					}
					
					processedInitializeCapCountRequest.setInitializeRequestStatus(InitializeRequestStatus.Backfill_Retry);
					if (initializeCapCountRequests.indexOf(processedInitializeCapCountRequest) == 0) {
					    scheduledDate = Calendar.getInstance().getTime(); 
						scheduledDate.setTime(processedInitializeCapCountRequest.getEffectiveDate().getTime() + CapCountService.getBackFillTimeOffsetInMilliSeconds());
						cakeTimer = capCountService.getAvailCakeTimer (scheduledDate.getTime());
						initCapCountTimerTask_Spark = capCountService.scheduleInitReqTimerTask(cakeTimer, scheduledDate, 
							processedInitializeCapCountRequest, ws_runCount, callerThreadId);
					} else {
						initCapCountTimerTask_Spark.addInitializeCapCountRequest(processedInitializeCapCountRequest);
					}					
					break;
					
				case Backfill_Done:
					cachedInitReqRDD.unpersist(false);										
					break;
					
				default: 
					logger.error("Invalid InitializeCapCountRequest Status: " + processedInitializeCapCountRequest.getInitializeRequestStatus() + 
						" - processedInitializeCapCountRequest: " + processedInitializeCapCountRequest);					
				}				
				logger.debug("done  " + msg);		
			}		        					
    	} catch (Throwable exc) {
    		logger.error("", exc);
    		cakeTimer.scheduleSharedTask(this, Calendar.getInstance().getTime());
    	}
		finally {
			cakeTimer.taskDone(this);
		}        		
	}
	
	private void handleMaxRetriesReached (SparkInitializeCapCountRequest processedInitializeCapCountRequest, 
		InitializeRequestStatus initializeRequestStatus, JavaRDD<SparkInitializeCapCountRequest> cachedInitReqRDD,
		CapCountWebService capCountWebService) throws Throwable {
		processedInitializeCapCountRequest.setInitializeRequestStatus(initializeRequestStatus);		
		CapCountWebService.getInstance(properties, topLevelClientsConfig);						
		capCountWebService.saveInitializeCapCountByWS(processedInitializeCapCountRequest);
		cachedInitReqRDD.unpersist(false);		
	}
}
