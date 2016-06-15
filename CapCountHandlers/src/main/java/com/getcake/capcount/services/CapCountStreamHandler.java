package com.getcake.capcount.services;

import static spark.Spark.post;
import spark.Spark;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import scala.Tuple2;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.getcake.capcount.model.CapCountReqKey;
import com.getcake.capcount.model.StreamCapCountRequest;
import com.getcake.capcount.model.CapEntity;
import com.getcake.capcount.model.CapEntityType;
import com.getcake.capcount.model.CountType;
import com.getcake.capcount.model.IncrementCapCountRequest;
import com.getcake.capcount.model.Iso8601JsonMapper;
import com.getcake.capcount.model.SparkIncrementCapCountRequest;
import com.getcake.util.AwsUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.fasterxml.jackson.databind.ObjectMapper;


public final class CapCountStreamHandler { 
	
	private static final Logger logger = Logger.getLogger(CapCountStreamHandler.class);

	static private String sparkMaster, appName, sparkConfigFileName;
	static private Config topLevelClientsConfig;
	static private String VERSION = "0.09";
	
	public static void main(String[] args) {
		Properties properties;
		String checkpointDirName, clientConfigFileName;
		Configuration hadoopConf;
		DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain;
		int countEnd, batchIntervalMilliSec;
		int streamSparkWebMinThreads, streamSparkWebMaxThreads, streamSparkWebIdleTimeoutMillis, streamSparkWebPort;
		JavaStreamingContext javaStreamingContext2;
		
		try {			
			logger.debug("VERSION: " + VERSION);
			if (args.length < 2) {
			  System.err.println(
				  "Usage: CapCountHandler <spark-config-file>  <client-meta-info.conf file>.\n"					  
			  );
			  System.exit(1);
			}

			sparkConfigFileName = args[0];
			logger.debug("debug sparkConfigFileName: " + sparkConfigFileName);
			logger.warn("sparkConfigFileName: " + sparkConfigFileName);
			properties = AwsUtil.loadProperties(sparkConfigFileName);

			sparkMaster = properties.getProperty("spark.master"); 
			appName = properties.getProperty("spark.app.name"); 
			String streamName =  properties.getProperty("streamName"); 
			countEnd = Integer.parseInt(properties.getProperty("countEnd"));
			batchIntervalMilliSec = Integer.parseInt(properties.getProperty("batchIntervalMilliSec"));
	        
			if (args[1].indexOf('/') == 0) {
			    clientConfigFileName =  args[1];
			} else {
			    clientConfigFileName =  Paths.get("").toAbsolutePath().toString() + "/" + args[1];				
			}
			topLevelClientsConfig = ConfigFactory.parseFile(new File(clientConfigFileName));
			
			SparkConf sparkConfig = new SparkConf();

			if (sparkMaster.indexOf("local") >= 0) {
				sparkConfig.setMaster(sparkMaster); 		    
				sparkConfig.setAppName(appName);
				sparkConfig.set("spark.executor.memory", properties.getProperty("spark.executor.memory"));
				sparkConfig.set("spark.driver.memory", properties.getProperty("spark.driver.memory"));
			} 	    

			checkpointDirName = properties.getProperty("checkpointDirectory"); 
			JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);	      
			defaultAWSCredentialsProviderChain = new DefaultAWSCredentialsProviderChain();

			JavaStreamingContextFactory javaStreamingContextFactory = new JavaStreamingContextFactory() {
				@Override
				public JavaStreamingContext create() {

					return createStreamingContext (javaSparkContext, 
							   defaultAWSCredentialsProviderChain, 
							   batchIntervalMilliSec,  checkpointDirName, streamName,
							   countEnd, properties, topLevelClientsConfig);
				}
			};

			sparkConfig.set("spark.hadoop.validateOutputSpecs", "false"); //allows saveAsHadoopFile to overwrite
			sparkConfig.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
			sparkConfig.set("spark.streaming.receiver.writeAheadLog.enable", "true");
			sparkConfig.set("spark.streaming.backpressure.enabled", "true"); 

			hadoopConf = javaSparkContext.hadoopConfiguration(); 
			hadoopConf.set("fs.s3.awsAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId()); 
			hadoopConf.set("fs.s3.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
			hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");

			hadoopConf.set("fs.s3n.awsAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId()); 
			hadoopConf.set("fs.s3n.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
			hadoopConf.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem");

			hadoopConf.set("fs.s3a.awsxxAccessKeyId", defaultAWSCredentialsProviderChain.getCredentials().getAWSAccessKeyId()); 
			hadoopConf.set("fs.s3a.awsSecretAccessKey", defaultAWSCredentialsProviderChain.getCredentials().getAWSSecretKey());
			hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
			hadoopConf.set("fs.s3a.connection.ssl.enabled","false");
			hadoopConf.set("fs.s3a.connection.maximum","false");

			logger.debug("checkpointDirectory: " + checkpointDirName);

			int numRetries = 0;
			while (true) {
				try {
				    javaStreamingContext2 = JavaStreamingContext.getOrCreate(checkpointDirName, hadoopConf, javaStreamingContextFactory);
				    break;
				} catch (Throwable exc) {
					if (++numRetries > 3) {
					  logger.error(numRetries + " times failed to create checkpoint directory at " + checkpointDirName, exc);
					  System.exit(-1);
					}
					logger.error("", exc);
					if (exc.getLocalizedMessage().contains("Failed to read checkpoint")) {
						File checkpointDir = new File (checkpointDirName);
						// TODO change to handle S3
						if (! checkpointDir.exists()) {
							if (!checkpointDir.mkdirs()) {
								throw new RuntimeException ("failed to create checkpoint directory at " + checkpointDir);
							}			
						}
						else if (! checkpointDir.delete()) {
							logger.error("deleting checkpointDir failed for " + checkpointDirName , exc);  								
						}
					}
				}				  
			}

			JavaStreamingContext javaStreamingContext = javaStreamingContext2;

			streamSparkWebMinThreads = Integer.parseInt(properties.getProperty("streamSparkWebMinThreads")); 
			streamSparkWebMaxThreads = Integer.parseInt(properties.getProperty("streamSparkWebMaxThreads")); 
			streamSparkWebIdleTimeoutMillis = Integer.parseInt(properties.getProperty("streamSparkWebIdleTimeoutMillis"));
			streamSparkWebPort = Integer.parseInt(properties.getProperty("streamSparkWebPort"));
			logger.debug("streamSparkWebPort: " + streamSparkWebPort);
			Spark.port(streamSparkWebPort);
			Spark.threadPool(streamSparkWebMaxThreads, streamSparkWebMinThreads, streamSparkWebIdleTimeoutMillis);

			post ("/capcounthandlers/stop", (request, response) -> {
				try {
					logger.debug("received /capcounthandlers/stop calling javaStreamingContext.stop(true, true); next ");
					javaStreamingContext.stop(true, true);
					logger.debug("javaStreamingContext.stop(true, true); done");
					System.exit(0);
					return "javaStreamingContext stopped";        		
				} catch (Throwable exc) {
					logger.error("", exc);
					return exc.getLocalizedMessage();
				}
			}); 

			// Start the streaming context and await termination
			javaStreamingContext.start();
			javaStreamingContext.awaitTermination();
		} catch (Throwable exc) {
			logger.error("main", exc);
		}    	
	}
  
  	static private JavaStreamingContext createStreamingContext (JavaSparkContext javaSparkContext, 
		DefaultAWSCredentialsProviderChain defaultAWSCredentialsProviderChain, 
		int batchIntervalMilliSec, String checkpointDirectory, String streamName, 
		int countEnd, Properties properties, Config topLevelClientsConfig) {	  
  		String endpointUrl;
		Duration batchInterval;	
	  	JavaStreamingContext javaStreamingContext;
	  	
	    batchInterval = new Duration(batchIntervalMilliSec);
	
	    Duration kinesisCheckpointInterval = batchInterval;
	  	javaStreamingContext = new JavaStreamingContext(javaSparkContext, batchInterval);  		    
	    javaStreamingContext.checkpoint(checkpointDirectory);
	
	    AmazonKinesisClient kinesisClient = new AmazonKinesisClient(defaultAWSCredentialsProviderChain); 	    
	    String regionName = properties.getProperty("region"); 	    
	    Region region = RegionUtils.getRegion(regionName);     
	    
	    endpointUrl = "https://kinesis." + region.getName() + ".amazonaws.com";
	    logger.debug("endpointUrl: " + endpointUrl + " - streamName: " + streamName);
	    kinesisClient.setEndpoint(endpointUrl);
	    
	    int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();	
	    int numStreams = numShards;
	
	    List<JavaDStream<byte[]>> streamsList = new ArrayList<JavaDStream<byte[]>>(numStreams);
	    String streamAppName = appName +  "-" + region + "-" + streamName;
	    logger.debug("streamAppName: " + streamAppName);
	    for (int i = 0; i < numStreams; i++) {	    	
		      streamsList.add(
		          KinesisUtils.createStream(javaStreamingContext, streamAppName, streamName, endpointUrl, regionName,
		              InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevels.MEMORY_ONLY)
		      );	      
	    }
	
	    // Union all the streams if there is more than 1 stream
	    JavaDStream<byte[]> unionStreams;
		logger.debug("# of shards streamsList.size(): " + streamsList.size());	    	  
	    if (streamsList.size() > 1) {
	      unionStreams = javaStreamingContext.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));	    	  
	    } else {
	      unionStreams = streamsList.get(0);
	    }
	
		unionStreams.checkpoint(kinesisCheckpointInterval); 		  			
		processBatchByReduceKeys (unionStreams, properties, topLevelClientsConfig);		
	    unionStreams.print();	    
	    return javaStreamingContext;
	}
  	
  	@SuppressWarnings("deprecation")
	static private void processBatchByReduceKeys (JavaDStream<byte[]> unionStreams, Properties properties_ReduceKeys_Param, Config topLevelClientsConfig_ReduceKeys_Param) {
  		
		JavaPairDStream<CapCountReqKey, CapCountReqKey> capCountReqPairs;
		JavaPairDStream<CapCountReqKey, CapCountReqKey> reducedCapCountReqPairs;
		
		boolean saveByWebServiceFlag;
		String saveByWebService = properties_ReduceKeys_Param.getProperty("saveByWebService");
		if (saveByWebService == null) {
			saveByWebServiceFlag = true;
		} else {
			saveByWebServiceFlag = Boolean.parseBoolean(saveByWebService);
		}
		
		capCountReqPairs = unionStreams.flatMapToPair(new PairFlatMapFunction<byte[], CapCountReqKey , CapCountReqKey>() {			
			Map<CapCountReqKey, CapCountReqKey> capCountKeyMap;			
			
			@Override
			public Iterable<Tuple2<CapCountReqKey, CapCountReqKey>> call(byte[] capReqBytes) throws Exception {
				StreamCapCountRequest capCountRequest;
				List<CapCountReqKey> capCountKeys;
				Tuple2<CapCountReqKey, CapCountReqKey> tuple;
				List<Tuple2<CapCountReqKey, CapCountReqKey>> tuples;
				String capCountRequestStr = null;
				ObjectMapper jsonMapper;
				try {
				    jsonMapper = Iso8601JsonMapper.getInstance();	    
					capCountKeyMap = new HashMap<>();
			        
					capCountRequestStr = new String (capReqBytes);
					capCountRequest = jsonMapper.readValue(capCountRequestStr, StreamCapCountRequest.class);
					capCountKeys = convertToCapCountKeys (capCountKeyMap, capCountRequest, capCountRequestStr);

					tuples = new ArrayList<>(capCountKeys.size());
					for (CapCountReqKey capCountKey : capCountKeys) {
						tuple = new Tuple2<CapCountReqKey, CapCountReqKey> (capCountKey, capCountKey);
						tuples.add(tuple);
					}
					return tuples;   
				} catch (Throwable exc) {
					logger.error("capCountRequestStr: " + capCountRequestStr, exc);
					throw exc;
				}
			}						
		});
				
		reducedCapCountReqPairs = capCountReqPairs.reduceByKey(new Function2<CapCountReqKey, CapCountReqKey, CapCountReqKey> () {

			@Override
			public CapCountReqKey call(CapCountReqKey capCountReqKey1, CapCountReqKey capCountReqKey2) throws Exception {				
				capCountReqKey1.mergeCapCountKey(capCountReqKey2);
				return capCountReqKey1; 
			}			
		});
		
		logger.warn ("reducedCapCountReqPairs.count(): " + reducedCapCountReqPairs.count());		
		
		reducedCapCountReqPairs.foreachRDD (new Function2<JavaPairRDD<CapCountReqKey, CapCountReqKey>, Time, Void> () {
			@Override
			public Void call (JavaPairRDD<CapCountReqKey, CapCountReqKey> rddPair, Time batchTime) throws Exception {
				
				logger.warn("rddPair.partitions().size(): " + rddPair.partitions().size());

					rddPair.foreachPartition (new VoidFunction<Iterator <Tuple2 <CapCountReqKey, CapCountReqKey>>> () {
						
						@Override
						public void call(Iterator<Tuple2<CapCountReqKey, CapCountReqKey>> capCountKeyValRddTuples) throws Exception {
							Tuple2<CapCountReqKey, CapCountReqKey> capCountKeyValRddTuple;
							String batchUuid;
							int partitionId;
							String partitionInfo = null;
							CapCountWebService capCountWebService = null;
							
							try {								
						        if (!capCountKeyValRddTuples.hasNext()) {
						        	return;
						        }
								partitionId = TaskContext.getPartitionId();
								
						        capCountWebService = CapCountWebService.getInstance(properties_ReduceKeys_Param, topLevelClientsConfig_ReduceKeys_Param, !saveByWebServiceFlag); 
																
						        batchUuid = "partitionId: " + partitionId + " - batchTime: " + batchTime;						        
								while (capCountKeyValRddTuples.hasNext()) {
									capCountKeyValRddTuple = capCountKeyValRddTuples.next();
									capCountKeyValRddTuple._1.setBatchUuid(batchUuid);
									capCountKeyValRddTuple._1.setPartitionId(partitionId);
									capCountKeyValRddTuple._1.setBatchTime(batchTime.milliseconds());
									capCountWebService.incrementCapCountByWS(getSparkIncrementCapCountReq (batchUuid, partitionId, batchTime, capCountKeyValRddTuple._1));										
								}     
							} catch (Throwable exc) {
								logger.error("*** Exception in " + partitionInfo, exc);
								if (exc instanceof Exception) {
									throw (Exception)exc;									
								} else {
									throw new Exception (exc);
								}
							}							
						}							
					});
				return null;
			}			
		});		
  	}  	

	static private SparkIncrementCapCountRequest getSparkIncrementCapCountReq (String batchUuid, int partitionId, Time batchTime,
		CapCountReqKey capCountReqKey) {
		SparkIncrementCapCountRequest sparkIncrementCapCountReq;
		IncrementCapCountRequest incrementCapCountReq; 
		List<StreamCapCountRequest> capCountRequests;
		
		sparkIncrementCapCountReq = new SparkIncrementCapCountRequest ();
		sparkIncrementCapCountReq.setBatchUuid(batchUuid);
		sparkIncrementCapCountReq.setPartitionId(partitionId);
		sparkIncrementCapCountReq.setBatchTime(batchTime);
		sparkIncrementCapCountReq.setMergedRequestUuids(capCountReqKey.getRequestUuids());
		capCountRequests = capCountReqKey.getCapCountRequests();
		incrementCapCountReq = capCountRequests.get(0).getIncrementCapCountReq();
		sparkIncrementCapCountReq.setCampaign(incrementCapCountReq.getCampaign());
		sparkIncrementCapCountReq.setOffer(incrementCapCountReq.getOffer());
		sparkIncrementCapCountReq.setOfferContract(incrementCapCountReq.getOfferContract());
		sparkIncrementCapCountReq.setGlobalEventReqInfo(incrementCapCountReq.getGlobalEventReqInfo());
		sparkIncrementCapCountReq.setClientId(incrementCapCountReq.getClientId());
		sparkIncrementCapCountReq.setRequestDate(incrementCapCountReq.getRequestDate());
		sparkIncrementCapCountReq.setIncrementCount(capCountReqKey.getAccIncrementCount());
		
		for (StreamCapCountRequest capCountRequest : capCountRequests  ) {
			sparkIncrementCapCountReq.addMergedRequestUuids(capCountRequest.getIncrementCapCountReq().getRequestUuid());				
		}
		
		return sparkIncrementCapCountReq;
	}	
  	
  	static private List<CapCountReqKey> convertToCapCountKeys (Map<CapCountReqKey, CapCountReqKey> capCountKeyMap, StreamCapCountRequest capCountRequest, String capCountRequestStr) {
  		List<CapCountReqKey> capCountKeys;
		CapCountReqKey capCountKey, existingCapCountKey;
		IncrementCapCountRequest incrementCapCountReq;
		CapEntity capEntity;
		
		capCountKeys = new ArrayList<>();		
		
		incrementCapCountReq = capCountRequest.getIncrementCapCountReq();
		if (incrementCapCountReq != null) {
			if (incrementCapCountReq.getOfferContract() != null) {
				capCountKey = new CapCountReqKey ();
				
				capEntity = incrementCapCountReq.getOfferContract();
				
				setCapCountKeyValues (incrementCapCountReq, capEntity, capCountKey);
				
				existingCapCountKey = capCountKeyMap.get(capCountKey);
				if (existingCapCountKey != null) {
					logger.debug("existingCapCountKey found: " + existingCapCountKey.getBatchUuid());
				} else {
					capCountKeyMap.put(capCountKey, capCountKey);
				}
				
				capCountKeys.add(capCountKey);
				capCountKey.addCapCountRequest(capCountRequest, capCountRequestStr);
			} 
			
			if (incrementCapCountReq.getOffer() != null) {
				capCountKey = new CapCountReqKey ();
				capEntity = incrementCapCountReq.getOffer();
				capCountKey.setEntityType(CapEntityType.Offer);															
				setCapCountKeyValues (incrementCapCountReq, capEntity, capCountKey);
				capCountKeys.add(capCountKey);
				capCountKey.addCapCountRequest(capCountRequest, capCountRequestStr);
			} 
			
			if (incrementCapCountReq.getCampaign() != null) {
				capCountKey = new CapCountReqKey ();
				capEntity = incrementCapCountReq.getCampaign();
				capCountKey.setEntityType(CapEntityType.Campaign);															
				setCapCountKeyValues (incrementCapCountReq, capEntity, capCountKey);
				capCountKeys.add(capCountKey);
				capCountKey.addCapCountRequest(capCountRequest, capCountRequestStr);
			} 
			
			if (incrementCapCountReq.getGlobalEventReqInfo() != null) {
				capCountKey = new CapCountReqKey ();
				capEntity = incrementCapCountReq.getGlobalEventReqInfo();
				capCountKey.setEntityType(CapEntityType.Global);															
				setCapCountKeyValues (incrementCapCountReq, capEntity, capCountKey);
				capCountKeys.add(capCountKey);
				capCountKey.addCapCountRequest(capCountRequest, capCountRequestStr);
			}
			
            return capCountKeys;
		} else if (capCountRequest.getInitializeCapCountReq() != null) {
			capCountKey = new CapCountReqKey ();
			capCountKey.addCapCountRequest(capCountRequest, capCountRequestStr);
            return capCountKeys;
		} else {
			return null;
		}  		
  	}
  	
  	static private void setCapCountKeyValues (IncrementCapCountRequest incCapCountRequest, CapEntity capEntity, CapCountReqKey capCountKey) {
		capCountKey.setClientId(incCapCountRequest.getClientId());
		capCountKey.setEntityType(capEntity.getCapEntityType());
		capCountKey.setEntityId(capEntity.getId());
		if (capEntity.getClick() != null) {
			capCountKey.setIntervalType(capEntity.getClick().getIntervalType());
			capCountKey.setCountType(CountType.CLICK);																
			capCountKey.setStartDate(capEntity.getClick().getStartDate());								
			capCountKey.setEndDate(capEntity.getClick().getEndDate());								
		} else if (capEntity.getGlobalEvent() != null) {
			capCountKey.setIntervalType(capEntity.getGlobalEvent().getIntervalType());
			capCountKey.setCountType(CountType.MACRO_EVENT);																
			capCountKey.setStartDate(capEntity.getMacroEvent().getStartDate());								
			capCountKey.setEndDate(capEntity.getMacroEvent().getEndDate());								
		} else if (capEntity.getMacroEvent() != null) {
			capCountKey.setIntervalType(capEntity.getMacroEvent().getIntervalType());
			capCountKey.setCountType(CountType.MACRO_EVENT);																
			capCountKey.setStartDate(capEntity.getMacroEvent().getStartDate());								
			capCountKey.setEndDate(capEntity.getMacroEvent().getEndDate());								
		} else if (capEntity.getMicroEvent() != null) {
			capCountKey.setIntervalType(capEntity.getMicroEvent().getIntervalType());
			capCountKey.setCountType(CountType.MICRO_EVENT);																
			capCountKey.setStartDate(capEntity.getMicroEvent().getStartDate());								
			capCountKey.setEndDate(capEntity.getMicroEvent().getEndDate());								
			capCountKey.setEventId(capEntity.getMicroEvent().getEventId());								
		}  		
  	}
  	
       	
 
}