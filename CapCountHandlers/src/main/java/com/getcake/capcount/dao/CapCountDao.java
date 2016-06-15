package com.getcake.capcount.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import org.apache.log4j.Logger;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.BatchStatement.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.capcount.model.ClickEventTypeKey;
import com.getcake.capcount.model.CapEntityMicroEvents;
import com.getcake.capcount.model.CapEntity;
import com.getcake.capcount.model.CapEntityDetails;
import com.getcake.capcount.model.ClickEventCounts;
import com.getcake.capcount.model.CapEntityType;
import com.getcake.capcount.model.ClientDaoCapKeyInfo;
import com.getcake.capcount.model.CountType;
import com.getcake.capcount.model.DBTransactionInfo;
import com.getcake.capcount.model.ClickEvent;
import com.getcake.capcount.model.MicroEvent;
import com.getcake.capcount.model.InitializeCapCountRequest;
import com.getcake.capcount.model.InitializeRequestStatus;
import com.getcake.capcount.model.IntervalType;
import com.getcake.capcount.model.KeySpaceInfo;
import com.getcake.capcount.model.PendingInitReqInfo;
import com.getcake.capcount.model.SparkIncrementCapCountRequest;
import com.getcake.capcount.model.SparkInitializeCapCountRequest;
import com.getcake.util.AwsUtil;
import com.getcake.util.NullValueSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import java.util.Iterator;

public class CapCountDao  implements Serializable {

	private static final long serialVersionUID = 1L;
	protected static final Logger logger = Logger.getLogger(CapCountDao.class);
	private static CapCountDao instance;

	static private final String ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX = "cap_entity_total_count_";
	static private final String ENTITY_DATE_TBL_NAME_PREFIX = "cap_entity_total_count_date_";
	
	static private final String MICRO_EVENT_TOTAL_COUNT_TBL_NAME_PREFIX = "micro_event_total_count_";
	static private final String MICRO_EVENT_DATE_TBL_NAME_PREFIX = "micro_event_total_count_date_";
	
	static private String UPDATE_SQL_CLAUSE = "UPDATE "; 
	static private String DELETE_SQL_CLAUSE = "DELETE FROM ";   
	static private String SELECT_SQL_CLAUSE = "SELECT * FROM ";   
	static private final String INSERT_INC_CAP_CLAUSE = 
	  "cap_count_request (created_date, client_id, batch_uuid, batch_time, spark_partition_id, request_uuids_hashcode, request_uuids, increment_count, request_date) values (unixTimestampOf (now()), ?, ?, ?, ?, ?, ?, ?, ?) "; // if not exists 
	
	static private final String DEL_INC_CAP_REQ_CLAUSE = "cap_count_request where batch_uuid  = ?";
	
	static private final String CAP_ENTITY_WHERE_CLAUSE = 
			" where entity_type_code = ? and entity_id = ? and interval_type_code = ? and start_date = ? and end_date = ?";
	static private final String MICRO_EVENT_WHERE_CLAUSE = 
			" where entity_type_code = ? and entity_id = ? and event_id = ? and interval_type_code = ? and start_date = ? and end_date = ?";

	static private final String MULTI_MICRO_EVENTS_WHERE_CLAUSE = 
			" where entity_type_code = ? and entity_id = ? and interval_type_code = ? and start_date = ? and end_date = ? and event_id in (";
	
	// init cap entity
	static private final String INIT_CAP_ENTITY_TBL_NAME_PREFIX = "init_cap_entity_";
	static private final String INIT_CAP_ENTITY_SET_CLAUSE = 
" set created_date = unixTimestampOf (now()), initial_value = ?, request_received_region = ?, primary_region = ?, primary_region_status = ?, primary_region_status_date = unixTimestampOf (now()), primary_region_retry_count = ?";

	static private final String INIT_CAP_ENTITY_SET_CLAUSE_SEC_REGION = 
" set created_date = unixTimestampOf (now()), initial_value = ?, request_received_region = ?, secondary_region = ?, secondary_region_status = ?, secondary_region_status_date = unixTimestampOf (now()), secondary_region_retry_count = ?";

	static private final String INIT_CAP_ENTITY_WHERE_CLAUSE = CAP_ENTITY_WHERE_CLAUSE + " and count_type_code = ? ";
	static private final String INIT_CAP_ENTITY_EFF_DATE_WHERE_CLAUSE = INIT_CAP_ENTITY_WHERE_CLAUSE + " and effective_date = ? ";
	static private final String INIT_CAP_ENTITY_PENDING_REQ_WHERE_CLAUSE = INIT_CAP_ENTITY_WHERE_CLAUSE + " and effective_date >= unixTimestampOf (now()) ";

	// Init micro
	static private final String INIT_MICRO_EVENT_TBL_NAME_PREFIX = "init_micro_event_";
	static private final String INIT_MICRO_EVENT_SET_CLAUSE = " set created_date = unixTimestampOf (now())";

	static private final String INIT_MICRO_EVENT_EFF_DATE_WHERE_CLAUSE = MICRO_EVENT_WHERE_CLAUSE + "  and effective_date = ? ";

	private Cluster cluster;	
	private Session masterKeySpaceSession;
	private String masterKeyspace, requestKeySpaceName;
	private PreparedStatement getKeySpacePerClientIdStmt;
	
	private Map<Integer, String> clientToCountKeySpaceNameMap;
	private Map<String, KeySpaceInfo> keySpaceNameToKeySpaceInfoMap; 
	private Map<Integer, ClientDaoCapKeyInfo> capKeyDaoInfoMap;
	private ObjectMapper jsonMapper;
	
	public static CapCountDao getInstance(Properties properties, Config topLevelClientsConfig, boolean initCassandraConnFlag) {
		
		if (instance != null ) {
			logger.debug("CapCountDao - getInstance instance not null on first check " +
					" - thread id " + Thread.currentThread().getId() +
					" instance: " + instance);			
			return instance;
		}
		
		synchronized (CapCountDao.class) {			
			logger.debug("CapCountDao - getInstance in sync " +
					" - thread id " + Thread.currentThread().getId());
			if (instance != null ) {
				logger.debug("CapCountService - getInstance instance in sync block not null on second check " +
						" - thread id " + Thread.currentThread().getId() +
						" instance: " + instance);
				return instance;
			}
			logger.debug("CapCountDao - getInstance instance in sync block before create new instance " + 
					" - thread id " + Thread.currentThread().getId());
			
			instance = new CapCountDao(topLevelClientsConfig, properties, initCassandraConnFlag);			
			logger.debug("CapCountDao - getInstance instance in sync block done created new instance " + 
					" - thread id " + Thread.currentThread().getId());
			return instance; 			
		}
		
	}	

	private CapCountDao(Config topLevelClientsConfig, Properties properties, boolean initCassandraConnFlag) {
		capKeyDaoInfoMap = new HashMap<>();
		keySpaceNameToKeySpaceInfoMap = new HashMap<>();
		init(topLevelClientsConfig, properties, initCassandraConnFlag);
	}

	private void init (Config topLevelClientsConfig, Properties properties, boolean initCassandraConnFlag) {
		String cassandraIPList, mssqlDbDriver, region;
		int numDefaultClientsPerRegion;
						
		region = AwsUtil.getAwsRegion();
		
		numDefaultClientsPerRegion = Integer.parseInt(properties.getProperty("numDefaultClientsPerRegion"));		
		clientToCountKeySpaceNameMap = new HashMap<>(numDefaultClientsPerRegion);

		jsonMapper = new ObjectMapper();
		jsonMapper.getSerializerProvider().setNullValueSerializer(new NullValueSerializer()); 		
		
		Config allClientsConfig;
		Set<String> keySet;
		String key;
		Config clientConfig;
		ConfigObject configObj;
		ClientDaoCapKeyInfo clientDaoCapKeyInfo;
		int clientId;
		MsSqlCapCountDao dbInfo;
		
		mssqlDbDriver = topLevelClientsConfig.getString("mssql-dbDriver");
		
		allClientsConfig = topLevelClientsConfig.getConfig("clients");
		
		configObj = allClientsConfig.root();  
		keySet = configObj.keySet();
		Iterator<String> iter = keySet.iterator();
		while (iter.hasNext()) {
			key = iter.next();
			clientConfig = allClientsConfig.getConfig(key);			
			clientDaoCapKeyInfo = new ClientDaoCapKeyInfo ();
			clientId = Integer.parseInt(key);
			clientDaoCapKeyInfo.setClientId(clientId);
			
			dbInfo = new MsSqlCapCountDao (); 
			dbInfo.setDbDriver(mssqlDbDriver);
			dbInfo.setDbJdbcUrl(clientConfig.getString("primary-sql-server.url"));
			dbInfo.setUserName(clientConfig.getString("primary-sql-server.user"));
			dbInfo.setPassword(clientConfig.getString("primary-sql-server.password"));
			clientDaoCapKeyInfo.setPrimarySqlServer(dbInfo);
			
			dbInfo = new MsSqlCapCountDao ();
			dbInfo.setDbDriver(mssqlDbDriver);
			dbInfo.setDbJdbcUrl(clientConfig.getString("standby-sql-server.url"));
			dbInfo.setUserName(clientConfig.getString("standby-sql-server.user"));
			dbInfo.setPassword(clientConfig.getString("standby-sql-server.password"));
			clientDaoCapKeyInfo.setStandbySqlServer(dbInfo);
			
    		this.capKeyDaoInfoMap.put(clientId, clientDaoCapKeyInfo);                				
		}		
		
		if (!initCassandraConnFlag) {
			return;
		}
		
		if (cluster != null) {
			logger.debug("CapCountDao already init Cassandra cluster: " + cluster + " for Thread id: " + Thread.currentThread().getId());			
			return;
		}
		
		cassandraIPList = properties.getProperty("cassandraIPList"); 
		cluster = Cluster.builder().addContactPoint(cassandraIPList).build();
		logger.debug("CapCountDao new init Cassandra cluster: " + cluster + " for Thread id: " + Thread.currentThread().getId());
		
		if (region == null || region.trim().length() == 0) {
			requestKeySpaceName = properties.getProperty("defaultRequestKeySpace");
		} else {
			requestKeySpaceName = region.replace ('-', '_');
			requestKeySpaceName = requestKeySpaceName + "_keyspace";			
		}
		
		masterKeyspace = properties.getProperty("masterKeyspaceName");
		masterKeySpaceSession = cluster.connect(masterKeyspace);
		getKeySpacePerClientIdStmt = masterKeySpaceSession.prepare("select * from client where client_id = ?"); 		 	
	}	
	
	public KeySpaceInfo getClientKeySpaceInfo (int clientId) {
		String countKeySpaceName;
		BoundStatement boundStmt;
		ResultSet clientInfoResultSet;
		
		countKeySpaceName = clientToCountKeySpaceNameMap.get(clientId);
		if (countKeySpaceName == null) {
			synchronized (clientToCountKeySpaceNameMap) {
				countKeySpaceName = clientToCountKeySpaceNameMap.get(clientId);
				if (countKeySpaceName == null) {
					boundStmt = getKeySpacePerClientIdStmt.bind(clientId);
					clientInfoResultSet = masterKeySpaceSession.execute(boundStmt);
					Row row = clientInfoResultSet.one();
					if (row == null) {
						throw new RuntimeException ("cakeapps_master_keyspace.client does not contain client id: " + clientId);
					}
					countKeySpaceName = row.getString("keyspace_name");
					clientToCountKeySpaceNameMap.put(clientId, countKeySpaceName);					
				}				
			}
		}		
		
		return getKeySpaceInfo (countKeySpaceName);
	}
	
	public KeySpaceInfo getKeySpaceInfo (String countKeySpaceName) {
		KeySpaceInfo keySpaceInfo;
		 
		keySpaceInfo = keySpaceNameToKeySpaceInfoMap.get(countKeySpaceName);
		if (keySpaceInfo != null) {
			return keySpaceInfo;
		}

		synchronized (keySpaceNameToKeySpaceInfoMap) {
			keySpaceInfo = keySpaceNameToKeySpaceInfoMap.get(countKeySpaceName);
			if (keySpaceInfo != null) {
				return keySpaceInfo;
			}
			
			keySpaceInfo = new KeySpaceInfo ();
			keySpaceInfo.setCountKeySpaceSession(cluster.connect(countKeySpaceName));
			keySpaceInfo.setCountKeySpace(countKeySpaceName);
			keySpaceNameToKeySpaceInfoMap.put(countKeySpaceName, keySpaceInfo);
			return keySpaceInfo;
		}		
	}	
	
	public DBTransactionInfo getDBTransactionInfo () {
		DBTransactionInfo dbTransactionInfo = new DBTransactionInfo ();
		
		dbTransactionInfo.batchUpsertIncCountStmt = new BatchStatement(Type.COUNTER);
		dbTransactionInfo.batchUpdateIncDateStmt = new BatchStatement(Type.LOGGED);
		dbTransactionInfo.batchInsertIncReqStmt = new BatchStatement(Type.LOGGED);
		
		dbTransactionInfo.batchDeleteCountStmt = new BatchStatement(Type.COUNTER);
		dbTransactionInfo.batchDeleteDateStmt = new BatchStatement(Type.LOGGED);
		return dbTransactionInfo;
	}
	
	public void processIncrementCapCount (KeySpaceInfo keySpaceInfo, 
		ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityCountInfo, 
		SparkIncrementCapCountRequest sparkIncrementCapCountReq, DBTransactionInfo dbTransactionInfo) throws Throwable {
		Session countKeySpaceSession;
		CapEntity capEntity;
	    BatchStatement batchUpsertIncCountStmt, batchUpdateIncDateStmt, batchInsertIncReqStmt, batchDeleteDateStmt, batchDeleteCountStmt;
	    ResultSet resultSet;
	    
		try {
	        countKeySpaceSession = keySpaceInfo.getCountKeySpaceSession(); 			
    		batchUpsertIncCountStmt = dbTransactionInfo.batchUpsertIncCountStmt;
    		batchUpdateIncDateStmt = dbTransactionInfo.batchUpdateIncDateStmt;
    		batchInsertIncReqStmt = dbTransactionInfo.batchInsertIncReqStmt;    		
    		batchDeleteCountStmt = dbTransactionInfo.batchDeleteCountStmt;
    		batchDeleteDateStmt = dbTransactionInfo.batchDeleteDateStmt;
    		  
    		capEntity = capEntityCountInfo.getCapEntity();
    		
			setupIncrementCountBoundStmts(clientDaoCapKeyInfo, batchInsertIncReqStmt, 
				batchUpsertIncCountStmt, batchUpdateIncDateStmt, sparkIncrementCapCountReq, capEntity);    		
    		resultSet = countKeySpaceSession.execute(batchInsertIncReqStmt);
			{ 
				try {
					if (batchDeleteCountStmt.getStatements().size() > 0) {
						resultSet = countKeySpaceSession.execute(batchDeleteCountStmt);
					}
					resultSet = countKeySpaceSession.execute(batchUpsertIncCountStmt);
				} catch (Throwable exc) {
					try {
						logger.error("Increment Request failed thread id: " + Thread.currentThread().getId() + " - " + sparkIncrementCapCountReq.getMergedRequestUuids(), exc);
						PreparedStatement deleteReqStmt = clientDaoCapKeyInfo.getDeleteReqStmt();
						BoundStatement deleteBoundStmt = deleteReqStmt.bind(sparkIncrementCapCountReq.getBatchUuid());
						resultSet = countKeySpaceSession.execute(deleteBoundStmt);
						this.dumpRSInfo(resultSet);
						
						return;
					} catch (Throwable exc2) {
						logger.error("Critical Err: failed to delete increment request " + sparkIncrementCapCountReq + "thread id: " + Thread.currentThread().getId(), exc2);
					}
				}
				
				try {
					
 					if (batchDeleteDateStmt.getStatements().size() > 0) {
 						resultSet = countKeySpaceSession.execute(batchDeleteDateStmt);
 					}					
					resultSet = countKeySpaceSession.execute(batchUpdateIncDateStmt);
				} catch (Throwable exc) {
					logger.error("Increment Date failed " + "thread id: " + Thread.currentThread().getId() + sparkIncrementCapCountReq.getMergedRequestUuids());
				}				
		    }   		    		
		} catch (Throwable exc) {
			logger.error("Err Insert increment request thread id: " + Thread.currentThread().getId() + " - " + sparkIncrementCapCountReq.getMergedRequestUuids(), exc);		
			throw exc;
		}		
	}
	
	public ClickEventCounts getClickEventCounts (ClientDaoCapKeyInfo clientDaoCapKeyInfo, Session countKeySpaceSession, 
		CapEntity capEntity,  ClickEvent clickEvent) {
		
		PreparedStatement selectCountStmt;		
		Date startDate, endDate;
		String entityTypeCode, intervalTypeCode;
		BoundStatement boundSelectInitStatement;
	    ResultSet countRS;
	    ClickEventCounts capEntityCounts;
	    
		selectCountStmt = clientDaoCapKeyInfo.getSelectCapEntityCountsStmt();		
		entityTypeCode = capEntity.getCapEntityType().getCode();
		intervalTypeCode = clickEvent.getIntervalType().getCode();
		
		startDate = clickEvent.getStartDate();
		endDate = clickEvent.getEndDate();

		boundSelectInitStatement = selectCountStmt.bind(entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate);
		countRS = countKeySpaceSession.execute(boundSelectInitStatement);
	    Row row = countRS.one();
		if (row == null) {
			return null;
		}
		
		capEntityCounts = new ClickEventCounts ();
		capEntityCounts.setClickCount(row.getLong("total_click_count"));
		capEntityCounts.setMacroEventCount(row.getLong("total_macro_count"));
		capEntityCounts.setGlobalEventCount(row.getLong("total_global_count"));
		
	    return capEntityCounts;	
	}
	
	public void getMicroEventCounts (ClientDaoCapKeyInfo clientDaoCapKeyInfo, Session countKeySpaceSession,
		CapEntityMicroEvents capEntityMicroEvents) {
		PreparedStatement selectCountStmt;
		Date startDate, endDate;
	    ResultSet countRS;
		List<MicroEvent> inputMicroEvents, microWEventsByType;
		ClickEventTypeKey clickEventTypeKey;
		Map<ClickEventTypeKey, List<MicroEvent>> microEventListMap;
		Map<MicroEvent, MicroEvent> microEventMap;
		StringBuilder eventIdList;
		MicroEvent microEventKey, microEvent;
		StringBuilder batchCountSql = new StringBuilder ();
		BoundStatement boundStatement;
	    Row row;
		Iterator<Row> resultIterator;
	    		
		inputMicroEvents = capEntityMicroEvents.getMicroEvents();
		if (inputMicroEvents == null) {
			return;
		}
		
		microEventListMap = new HashMap<>();
		for (MicroEvent localMicroEvent : inputMicroEvents) {
			clickEventTypeKey = new ClickEventTypeKey ();
			clickEventTypeKey.setIntervalType(localMicroEvent.getIntervalType());
			clickEventTypeKey.setStartDate(localMicroEvent.getStartDate());
			clickEventTypeKey.setEndDate(localMicroEvent.getEndDate());
			microWEventsByType = microEventListMap.get(clickEventTypeKey);
			if (microWEventsByType == null) {
				microWEventsByType = new ArrayList<>();
				microWEventsByType.add(localMicroEvent);
				microEventListMap.put(clickEventTypeKey, microWEventsByType);
			} else {
				microWEventsByType.add(localMicroEvent);
			}
		}
		
		microEventMap = new HashMap<>();
		
		for (Entry<ClickEventTypeKey, List<MicroEvent>> entryMicroEventDetails : microEventListMap.entrySet()) {
			clickEventTypeKey = entryMicroEventDetails.getKey();
			microWEventsByType = entryMicroEventDetails.getValue();
			eventIdList = new StringBuilder ();
			for (MicroEvent microEventLocal : microWEventsByType) {
				if (eventIdList.length() > 0) {
					eventIdList.append(',');					
				}
				eventIdList.append(microEventLocal.getEventId());
				microEventMap.put(microEventLocal, microEventLocal);
			}
			
			selectCountStmt = clientDaoCapKeyInfo.getSelectMultiMicroEventCountStmt();
			batchCountSql.delete(0, batchCountSql.length());
			batchCountSql.append(SELECT_SQL_CLAUSE);
			batchCountSql.append(clientDaoCapKeyInfo.getCountKeySpace());
			batchCountSql.append('.');
			batchCountSql.append(MICRO_EVENT_TOTAL_COUNT_TBL_NAME_PREFIX);
			batchCountSql.append(clientDaoCapKeyInfo.getClientId());
			batchCountSql.append(MULTI_MICRO_EVENTS_WHERE_CLAUSE);
			batchCountSql.append(eventIdList.toString());
			batchCountSql.append(')');
			selectCountStmt = countKeySpaceSession.prepare(batchCountSql.toString());
			
			startDate = clickEventTypeKey.getStartDate();
			endDate = clickEventTypeKey.getEndDate();
			
			boundStatement = selectCountStmt.bind(capEntityMicroEvents.getCapEntityType().getCode(), capEntityMicroEvents.getId(), 
					clickEventTypeKey.getIntervalType().getCode(), startDate, endDate);			
			// " where entity_type_code = ? and entity_id = ? and interval_type_code = ? and start_date = ? and end_date = ? and event_id in (?) ";
			countRS = countKeySpaceSession.execute(boundStatement);
			resultIterator = countRS.iterator();		    
			microEventKey = new MicroEvent ();
			while (resultIterator.hasNext()) { 
				row = resultIterator.next();
				microEventKey.setIntervalType(IntervalType.getIntervalType(row.getString("interval_type_code")));
				microEventKey.setStartDate(row.getDate("start_date"));
				microEventKey.setEndDate(row.getDate("end_date"));
				microEventKey.setEventId(row.getInt("event_id"));
				microEvent = microEventMap.get(microEventKey);
				microEvent.setCount(row.getLong("total_micro_event_count"));
			}			
		}
	} 
	
	private void dumpRSInfo (ResultSet resultSet) {
		Row row;
		ColumnDefinitions colDefs = resultSet.getColumnDefinitions();
		List<Definition> colDefList = colDefs.asList();
		for (Definition colDef : colDefList) {
			logger.debug("col name: " + colDef.getName());
		}
		Iterator<Row> rows =  resultSet.iterator();
		
		Object obj;
		while (rows.hasNext()) {
			row = rows.next();
			for (int i = 0; i < colDefList.size(); i++) {
				obj = row.getObject(i);
				logger.debug(obj);
			}
		}		
	}
	
	public void createInitCapEntity () {
		
	}
	
	public PendingInitReqInfo getInitCapEntityCount (
		    KeySpaceInfo keySpaceInfo,
		    ClientDaoCapKeyInfo clientDaoCapKeyInfo,
		    CapEntityDetails capEntityCountInfo) {
		
		Session countKeySpaceSession; 
		CountType countType;		
		PendingInitReqInfo pendingInitReqInfo;
		CapEntity capEntity;
		Date startDate, endDate, effectiveDate;
		BoundStatement boundSelectInitStatement = null;
		ClickEvent clickEvent = capEntityCountInfo.getClickEvent();
	    ResultSet initCountRS;
	    PreparedStatement selectInitStmt;
	    
		startDate = clickEvent.getStartDate();
		endDate = clickEvent.getEndDate();

		countKeySpaceSession = keySpaceInfo.getCountKeySpaceSession();
		countType = capEntityCountInfo.getCountType();
		capEntity = capEntityCountInfo.getCapEntity();
		if (CountType.MICRO_EVENT != capEntityCountInfo.getCountType() ) {
	    	selectInitStmt = clientDaoCapKeyInfo.getSelectPendingInitCapEntityReqStmt();        	
			boundSelectInitStatement = selectInitStmt.bind(capEntity.getCapEntityType().getCode(), capEntity.getId(),  
					capEntityCountInfo.getClickEvent().getIntervalType().getCode(), startDate, endDate, countType.getCode());			
		} else {
			// To do 
			return null;
		}
		
		initCountRS = countKeySpaceSession.execute(boundSelectInitStatement);
	    Row row = initCountRS.one();
		if (row == null) {
			return null;
		}
		
		effectiveDate = row.getDate("effective_date");
		pendingInitReqInfo = new PendingInitReqInfo ();
		pendingInitReqInfo.setEffectiveDate(effectiveDate);
		// pendingInitReqInfo.setInitCount(row.getLong("initial_value"));
		
	    return pendingInitReqInfo;	    
	}		

	
	public void deletePrevInitCapEntityCountRequests (ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityCountInfo, 
			Session countKeySpaceSession, Date newEffectiveDate) {
		
		// To do: skip for now because the UI prevents initialize request within 5 minutes after prev init request.
		// otherwise filter by effective date and status
		CapEntity capEntity;
		Date startDate, endDate;
		BoundStatement boundSelectInitStatement;
	    ResultSet initCountRS;
	    PreparedStatement selectInitStmt, deleteInitStmt;
	    BatchStatement batchStatement;
		ClickEvent clickEvent;
		CountType countType;
	    
		capEntity = capEntityCountInfo.getCapEntity();
		clickEvent = capEntityCountInfo.getClickEvent();
		startDate = clickEvent.getStartDate();
		endDate = clickEvent.getEndDate();
		if (endDate == null) {
			endDate = startDate;
		}
		countType = capEntityCountInfo.getCountType();
		String entityTypeCode = capEntity.getCapEntityType().getCode();
		String intervalTypeCode = clickEvent.getIntervalType().getCode();
    	selectInitStmt = clientDaoCapKeyInfo.getSelectPendingInitCapEntityReqStmt();        	
		boundSelectInitStatement = selectInitStmt.bind(entityTypeCode, capEntity.getId(),  
			intervalTypeCode, startDate, endDate, countType.getCode());
		
		initCountRS = countKeySpaceSession.execute(boundSelectInitStatement);
		Iterator<Row> initCountIter = initCountRS.iterator();
		Row row;
		deleteInitStmt = clientDaoCapKeyInfo.getDeleteInitCapEntityReqStmt();
		batchStatement = new BatchStatement ();
		while (initCountIter.hasNext()) {
			row = initCountIter.next();
			batchStatement.add(deleteInitStmt.bind(entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate, countType.getCode(), row.getDate("effective_date")));
		}
		if (batchStatement.getStatements().size() > 0) {
			countKeySpaceSession.execute (batchStatement);			
		}
	}		
	
	public void saveInitCapCountReq (KeySpaceInfo keySpaceInfo, ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityCountInfo, 
		SparkInitializeCapCountRequest initializeCapCountRequest, String requestReceivedRegion, String primaryRegion,
		InitializeRequestStatus initializeCapRequestStatus, int initializeCapRequestRetryCount) throws Throwable {
		
	    BatchStatement batchUpsertInitReqStmt;
	    Session clientKeySpaceSession;

		try {
		    /* clientId = initializeCapCountRequest.getClientId();
		    keySpaceInfo = getClientKeySpaceInfo(clientId); 
		    countKeySpaceSession = keySpaceInfo.getCountKeySpaceSession(); */
		    // clientKeySpaceSession = clientDaoCapKeyInfo.getClientKeySpaceSession();
		    clientKeySpaceSession = keySpaceInfo.getCountKeySpaceSession();
	        
			batchUpsertInitReqStmt = new BatchStatement(Type.LOGGED);
			setupInitCountBoundStmts(clientDaoCapKeyInfo, batchUpsertInitReqStmt, initializeCapCountRequest, capEntityCountInfo,
				requestReceivedRegion, primaryRegion, initializeCapCountRequest.getInitializeRequestStatus(), initializeCapCountRequest.getNumRetries());
			clientKeySpaceSession.execute(batchUpsertInitReqStmt);			
		} catch (Throwable exc) {
			logger.error("Err saving Initialize Count Request: " + initializeCapCountRequest, exc);
			throw exc;
		}		
	}
			
	public ClientDaoCapKeyInfo getClientCapKeyDaoInfo (int clientId) {
		KeySpaceInfo keySpaceInfo;
		ClientDaoCapKeyInfo clientDaoCapKeyInfo;

		clientDaoCapKeyInfo = this.capKeyDaoInfoMap.get(clientId);
        if (clientDaoCapKeyInfo == null) {
        	throw new RuntimeException ("Missing ClientCapKeyDaoInfo from client-meta-info.conf for client id " + clientId);
        }	        
        
        if (clientDaoCapKeyInfo.getSelectCapEntityCountsStmt() == null) {
        	synchronized (clientDaoCapKeyInfo) {
                if (clientDaoCapKeyInfo.getSelectCapEntityCountsStmt() == null) {
                	keySpaceInfo = getClientKeySpaceInfo(clientId);
                	initDaoCapKeyInfo(keySpaceInfo, requestKeySpaceName, clientId, clientDaoCapKeyInfo);
                }
        	}
        }
        return clientDaoCapKeyInfo;
	}
	
	public ClientDaoCapKeyInfo getClientCapKeyDaoInfo (int clientId, boolean initDBConnFlag) {
		KeySpaceInfo keySpaceInfo;
		ClientDaoCapKeyInfo clientDaoCapKeyInfo;

		clientDaoCapKeyInfo = this.capKeyDaoInfoMap.get(clientId);
        if (clientDaoCapKeyInfo == null) {
        	throw new RuntimeException ("Missing ClientCapKeyDaoInfo from client-meta-info.conf for client id " + clientId);
        }	        
        
        if (!initDBConnFlag) {
        	return clientDaoCapKeyInfo;
        }
        
        if (clientDaoCapKeyInfo.getSelectCapEntityCountsStmt() == null) {
        	synchronized (clientDaoCapKeyInfo) {
                if (clientDaoCapKeyInfo.getSelectCapEntityCountsStmt() == null) {
                	keySpaceInfo = getClientKeySpaceInfo(clientId);
                	initDaoCapKeyInfo(keySpaceInfo, requestKeySpaceName, clientId, clientDaoCapKeyInfo);
                }
        	}
        }
        return clientDaoCapKeyInfo;
	}
	
	private void setupIncrementCountBoundStmts (ClientDaoCapKeyInfo clientDaoCapKeyInfo,
		BatchStatement batchInsertIncReqStmt, BatchStatement batchUpsertIncCountStmt, BatchStatement batchUpdateIncDateStmt,
		SparkIncrementCapCountRequest sparkIncrementCapCountReq, CapEntity capEntity) throws JsonProcessingException {
		Date startDate, endDate, batchDate;
		ClickEvent clickEvent;
		String entityTypeCode, intervalTypeCode;
		MicroEvent microEvent;
		PreparedStatement updateCountStmt, updateDateStmt, insertReqStmt;
		CountType countType = null;
		int clientId = sparkIncrementCapCountReq.getClientId();
		long incrementCount = sparkIncrementCapCountReq.getIncrementCount();
		CapEntityType capEntityType = capEntity.getCapEntityType();
		String requestUuids[];
				
		entityTypeCode = capEntityType.getCode();
		
		insertReqStmt = clientDaoCapKeyInfo.getInsertReqStmt();
		batchDate = new Date (sparkIncrementCapCountReq.getBatchTime().milliseconds());
		Set<String> requestUuidSet = new HashSet<String>(sparkIncrementCapCountReq.getMergedRequestUuids());
		
		clickEvent = capEntity.getClick();
		if (clickEvent != null) {
			intervalTypeCode = clickEvent.getIntervalType().getCode();
			countType = CountType.CLICK;						
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();
			
        	updateCountStmt = clientDaoCapKeyInfo.getUpdateClickCountStmt();
        	updateDateStmt = clientDaoCapKeyInfo.getUpdateClickCountDateStmt();
			
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertIncCountStmt.add(updateCountStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
			batchUpdateIncDateStmt.add(updateDateStmt.bind(entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
		} 

		clickEvent = capEntity.getMacroEvent();
		if (clickEvent != null) {
			intervalTypeCode = clickEvent.getIntervalType().getCode();
			countType = CountType.MACRO_EVENT;
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();
			
        	updateCountStmt = clientDaoCapKeyInfo.getUpdateMacroEventCountStmt();
        	updateDateStmt = clientDaoCapKeyInfo.getUpdateMacroEventCountDateStmt();
			
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertIncCountStmt.add(updateCountStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
			batchUpdateIncDateStmt.add(updateDateStmt.bind(entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
		} 		

		clickEvent = capEntity.getGlobalEvent();
		if (clickEvent != null) {
			intervalTypeCode = clickEvent.getIntervalType().getCode();
			countType = CountType.GLOBAL;			
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();

        	updateCountStmt = clientDaoCapKeyInfo.getUpdateGlobalEventCountStmt();
        	updateDateStmt = clientDaoCapKeyInfo.getUpdateGlobalEventCountDateStmt();
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertIncCountStmt.add(updateCountStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
			batchUpdateIncDateStmt.add(updateDateStmt.bind(entityTypeCode, capEntity.getId(), intervalTypeCode, startDate, endDate));
		} 				

		microEvent = capEntity.getMicroEvent();
		if (microEvent != null) {
			intervalTypeCode = microEvent.getIntervalType().getCode();
			countType = CountType.MICRO_EVENT;
			startDate = microEvent.getStartDate();
			endDate = microEvent.getEndDate();

        	updateCountStmt = clientDaoCapKeyInfo.getUpdateMicroEventCountStmt();
        	updateDateStmt = clientDaoCapKeyInfo.getUpdateMicroEventCountDateStmt();        	
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertIncCountStmt.add(updateCountStmt.bind(incrementCount, entityTypeCode, capEntity.getId(),  microEvent.getEventId(), intervalTypeCode, startDate, endDate));
			batchUpdateIncDateStmt.add(updateDateStmt.bind(entityTypeCode, capEntity.getId(), microEvent.getEventId(), intervalTypeCode, startDate, endDate));
		} 				

		if (countType == null) {
			// error no count type is specified. TODO handle error
			throw new RuntimeException ("error no count type is specified for request " + sparkIncrementCapCountReq);
		}		
		
		logger.debug("batchInsertIncReqStmt for batchUuid: " + sparkIncrementCapCountReq.getBatchUuid());
		
		requestUuids = new String [sparkIncrementCapCountReq.getMergedRequestUuids().size()];
		requestUuids = sparkIncrementCapCountReq.getMergedRequestUuids().toArray(requestUuids);
		String requestUuidsStr = Arrays.toString(requestUuids);
		
		/*
		StringBuilder requestUuidsStr = new StringBuilder ();
		Iterator<String> iterator = requestUuidSet.iterator();
		while (iterator.hasNext()) {
			requestUuidsStr.append(iterator.next());
		}
		*/
		
		batchInsertIncReqStmt.add(insertReqStmt.bind(clientId, sparkIncrementCapCountReq.getBatchUuid(), 
			batchDate, sparkIncrementCapCountReq.getPartitionId(), requestUuidsStr.toString().hashCode(),
			requestUuidSet, incrementCount, sparkIncrementCapCountReq.getRequestDate()));		
	}

	private void initDaoCapKeyInfo (KeySpaceInfo keySpaceInfo, String requestKeySpace, int clientId, 
		ClientDaoCapKeyInfo clientDaoCapKeyInfo) {
		String countKeySpace = keySpaceInfo.getCountKeySpace(); 
		Session countKeySpaceSession = keySpaceInfo.getCountKeySpaceSession();
		StringBuilder batchCountSql, batchDateSql, insertReqSql, delReqSql, initializeReqSql, selectSql;
		PreparedStatement countStmt, updateDateStmt, insertReqStmt, deleteReqStmt, initCapCountStmt, 
		  deleteInitializeReqStmt, selectCountsStmt;
		
		batchCountSql = new StringBuilder (); 
		batchDateSql = new StringBuilder (); 
		insertReqSql = new StringBuilder ();
		delReqSql = new StringBuilder ();
		initializeReqSql = new StringBuilder ();
		selectSql = new StringBuilder ();

        clientDaoCapKeyInfo.setClientId(clientId);
        clientDaoCapKeyInfo.setCountKeySpace(countKeySpace);
        clientDaoCapKeyInfo.setRequestKeySpace(requestKeySpace);
        
        // Click
		batchCountSql.append(UPDATE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(" set total_click_count = total_click_count + ?");
		batchCountSql.append(CAP_ENTITY_WHERE_CLAUSE);			
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setUpdateClickCountStmt(countStmt);
		
		batchDateSql.append(UPDATE_SQL_CLAUSE);
		batchDateSql.append(countKeySpace);
		batchDateSql.append('.');
		batchDateSql.append(ENTITY_DATE_TBL_NAME_PREFIX);
		batchDateSql.append(clientId);
		batchDateSql.append(" set total_click_count_updated_date = unixTimestampOf(now ())");
		batchDateSql.append(CAP_ENTITY_WHERE_CLAUSE);
		updateDateStmt = countKeySpaceSession.prepare(batchDateSql.toString());
		clientDaoCapKeyInfo.setUpdateClickCountDateStmt(updateDateStmt);

		// MACRO_EVENT
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(UPDATE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(" set total_macro_count = total_macro_count + ?");
		batchCountSql.append(CAP_ENTITY_WHERE_CLAUSE);
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setUpdateMacroEventCountStmt(countStmt);
		
		batchDateSql.delete(0,  batchDateSql.length());
		batchDateSql.append(UPDATE_SQL_CLAUSE);
		batchDateSql.append(countKeySpace);
		batchDateSql.append('.');
		batchDateSql.append(ENTITY_DATE_TBL_NAME_PREFIX);
		batchDateSql.append(clientId);
		batchDateSql.append(" set total_macro_count_updated_date = unixTimestampOf (now ())");
		batchDateSql.append(CAP_ENTITY_WHERE_CLAUSE);
		updateDateStmt = countKeySpaceSession.prepare(batchDateSql.toString());
		clientDaoCapKeyInfo.setUpdateMacroEventCountDateStmt(updateDateStmt);

		// Global_EVENT
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(UPDATE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(" set total_global_count = total_global_count + ?");
		batchCountSql.append(CAP_ENTITY_WHERE_CLAUSE);
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setUpdateGlobalEventCountStmt(countStmt);
		
		batchDateSql.delete(0,  batchDateSql.length());
		batchDateSql.append(UPDATE_SQL_CLAUSE);
		batchDateSql.append(countKeySpace);
		batchDateSql.append('.');
		batchDateSql.append(ENTITY_DATE_TBL_NAME_PREFIX);
		batchDateSql.append(clientId);
		batchDateSql.append(" set total_global_count_updated_date = unixTimestampOf (now ())");
		batchDateSql.append(CAP_ENTITY_WHERE_CLAUSE);
		updateDateStmt = countKeySpaceSession.prepare(batchDateSql.toString());
		clientDaoCapKeyInfo.setUpdateGlobalEventCountDateStmt(updateDateStmt);

		// MICRO_EVENT
		/* To do
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(SELECT_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(MICRO_EVENT_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(MULTI_MICRO_EVENTS_WHERE_CLAUSE);			
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setSelectMultiMicroEventCountStmt(countStmt);
		*/
		
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(UPDATE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(MICRO_EVENT_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(" set total_micro_event_count = total_micro_event_count + ?");
		batchCountSql.append(MICRO_EVENT_WHERE_CLAUSE);			
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setUpdateMicroEventCountStmt(countStmt);

		batchDateSql.delete(0,  batchCountSql.length());
		batchDateSql.append(UPDATE_SQL_CLAUSE);
		batchDateSql.append(countKeySpace);
		batchDateSql.append('.');
		batchDateSql.append(MICRO_EVENT_DATE_TBL_NAME_PREFIX);
		batchDateSql.append(clientId);
		batchDateSql.append(" set total_micro_event_count_updated_date = unixTimestampOf(now ())");
		batchDateSql.append(MICRO_EVENT_WHERE_CLAUSE);
		updateDateStmt = countKeySpaceSession.prepare(batchDateSql.toString());
		clientDaoCapKeyInfo.setUpdateMicroEventCountDateStmt(updateDateStmt);

		// insert Increment request
		insertReqSql.append("insert into ");
		insertReqSql.append(requestKeySpace); 
		insertReqSql.append('.');
		insertReqSql.append(INSERT_INC_CAP_CLAUSE);
		insertReqStmt = countKeySpaceSession.prepare(insertReqSql.toString());
		clientDaoCapKeyInfo.setInsertReqStmt(insertReqStmt);

		// delete Increment request
		delReqSql.append("delete from ");
		delReqSql.append(requestKeySpace); 
		delReqSql.append('.');
		delReqSql.append(DEL_INC_CAP_REQ_CLAUSE);
		deleteReqStmt = countKeySpaceSession.prepare(delReqSql.toString());
		clientDaoCapKeyInfo.setDeleteReqStmt(deleteReqStmt);
				
		// Delete Cap Entity Count
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(DELETE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(CAP_ENTITY_WHERE_CLAUSE);
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setDeleteCapEntityCountStmt(countStmt);
		
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(DELETE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(ENTITY_DATE_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(CAP_ENTITY_WHERE_CLAUSE);
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setDeleteCapEntityDateStmt(countStmt);
		
		// Delete Micro Event Count
		batchCountSql.delete(0,  batchCountSql.length());
		batchCountSql.append(DELETE_SQL_CLAUSE);
		batchCountSql.append(countKeySpace);
		batchCountSql.append('.');
		batchCountSql.append(MICRO_EVENT_TOTAL_COUNT_TBL_NAME_PREFIX);
		batchCountSql.append(clientId);
		batchCountSql.append(MICRO_EVENT_WHERE_CLAUSE);
		countStmt = countKeySpaceSession.prepare(batchCountSql.toString());
		clientDaoCapKeyInfo.setDeleteMicroEventCountStmt(countStmt);		
		
		// init cap count
		initializeReqSql.append(SELECT_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_CAP_ENTITY_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_CAP_ENTITY_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setSelectInitCapEntityReqStmt(initCapCountStmt);				
		
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(UPDATE_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_CAP_ENTITY_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_CAP_ENTITY_SET_CLAUSE);
		initializeReqSql.append(INIT_CAP_ENTITY_EFF_DATE_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setUpsertInitCapEntityReqStmt(initCapCountStmt);
		
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(UPDATE_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_CAP_ENTITY_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_CAP_ENTITY_SET_CLAUSE_SEC_REGION);
		initializeReqSql.append(INIT_CAP_ENTITY_EFF_DATE_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setupsertInitCapEntityReqStmt_Sec_Region(initCapCountStmt);
		
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(SELECT_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_CAP_ENTITY_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_CAP_ENTITY_PENDING_REQ_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setSelectPendingInitCapEntityReqStmt(initCapCountStmt);

		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(DELETE_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_CAP_ENTITY_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_CAP_ENTITY_EFF_DATE_WHERE_CLAUSE);
		deleteInitializeReqStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setDeleteInitCapEntityReqStmt(deleteInitializeReqStmt);

		// init micro event
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(SELECT_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_MICRO_EVENT_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(MICRO_EVENT_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setSelectInitMicroEventReqStmt(initCapCountStmt);
						
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(UPDATE_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_MICRO_EVENT_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(INIT_MICRO_EVENT_SET_CLAUSE);
		initializeReqSql.append(INIT_MICRO_EVENT_EFF_DATE_WHERE_CLAUSE);
		initCapCountStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setUpsertInitMicroEventReqStmt(initCapCountStmt);
				
		initializeReqSql.delete(0, initializeReqSql.length());
		initializeReqSql.append(DELETE_SQL_CLAUSE);
		initializeReqSql.append(countKeySpace);
		initializeReqSql.append('.');
		initializeReqSql.append(INIT_MICRO_EVENT_TBL_NAME_PREFIX);
		initializeReqSql.append(clientId);		
		initializeReqSql.append(MICRO_EVENT_WHERE_CLAUSE);
		deleteInitializeReqStmt = countKeySpaceSession.prepare(initializeReqSql.toString());
		clientDaoCapKeyInfo.setDeleteInitMicroEventReqStmt(deleteInitializeReqStmt);

		selectSql.delete(0, selectSql.length());
		selectSql.append(SELECT_SQL_CLAUSE);
		selectSql.append(countKeySpace);
		selectSql.append('.');
		selectSql.append(ENTITY_TOTAL_COUNT_TBL_NAME_PREFIX);
		selectSql.append(clientId);		
		selectSql.append(CAP_ENTITY_WHERE_CLAUSE);
		selectCountsStmt = countKeySpaceSession.prepare(selectSql.toString());
		clientDaoCapKeyInfo.setSelectCapEntityCountsStmt(selectCountsStmt);

		selectSql.delete(0, selectSql.length());
		selectSql.append(SELECT_SQL_CLAUSE);
		selectSql.append(countKeySpace);
		selectSql.append('.');
		selectSql.append(MICRO_EVENT_DATE_TBL_NAME_PREFIX);
		selectSql.append(clientId);		
		selectSql.append(MICRO_EVENT_WHERE_CLAUSE);
		selectCountsStmt = countKeySpaceSession.prepare(selectSql.toString());
		clientDaoCapKeyInfo.setSelectMicroEventCountStmt(selectCountsStmt);
	}
	
	private void setupInitCountBoundStmts (ClientDaoCapKeyInfo clientDaoCapKeyInfo, BatchStatement batchUpsertInitReqStmt, 
		InitializeCapCountRequest initializeCapCountRequest, CapEntityDetails capEntityCountInfo, String requestReceivedRegion, 
		String primaryRegion, InitializeRequestStatus initializeCapRequestStatus, int initializeCapRequestRetryCount) throws JsonProcessingException {
		
		Date startDate, endDate;
		ClickEvent clickEvent;
		String entityTypeCode, intervalTypeCode;
		MicroEvent eventReqInfo;
		CountType countType = null;
		PreparedStatement upsertInitCapEntityReqStmt, upsertInitMicroEventReqStmt;
		CapEntityType capEntityType;
		CapEntity capEntity;

		capEntity = capEntityCountInfo.getCapEntity();
		capEntityType = capEntity.getCapEntityType();
		entityTypeCode  = capEntityType.getCode();

        upsertInitCapEntityReqStmt = clientDaoCapKeyInfo.getUpsertInitCapEntityReqStmt();
        upsertInitMicroEventReqStmt = clientDaoCapKeyInfo.getUpsertInitMicroEventReqStmt();
        
        clickEvent = capEntityCountInfo.getClickEvent();
		intervalTypeCode = clickEvent.getIntervalType().getCode();
		startDate = clickEvent.getStartDate();
		endDate = clickEvent.getEndDate();			
		if (endDate == null) {
			endDate = startDate;
		} 			
        countType = capEntityCountInfo.getCountType();
        switch (countType) {
        case CLICK:
        case MACRO_EVENT:
        case GLOBAL:
			batchUpsertInitReqStmt.add(upsertInitCapEntityReqStmt.bind(initializeCapCountRequest.getIncrementCount(), requestReceivedRegion, primaryRegion, initializeCapRequestStatus.getName(), initializeCapRequestRetryCount, entityTypeCode, capEntity.getId(), 
				intervalTypeCode, startDate, endDate, countType.getCode(), initializeCapCountRequest.getEffectiveDate()));
        	break;
        	
        default: // MICRO_EVENT
    		eventReqInfo = capEntity.getMicroEvent();
    		// " set created_date = unixTimestampOf (now()), request_received_region = ?, primary_region = ?, primary_region_status = ?, primary_region_status_date = unixTimestampOf (now()), primary_region_retry_count = ?";    		
			batchUpsertInitReqStmt.add(upsertInitMicroEventReqStmt.bind(initializeCapCountRequest.getIncrementCount(), requestReceivedRegion, primaryRegion, initializeCapRequestStatus.getName(), initializeCapRequestRetryCount, entityTypeCode, capEntity.getId(), 
				eventReqInfo.getEventId(),	intervalTypeCode, startDate, endDate, initializeCapCountRequest.getEffectiveDate()));
        }
        
        /*
        if (clickEvent != null) {
			countType = CountType.CLICK;						
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();			
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertInitReqStmt.add(upsertInitCapEntityReqStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), 
				intervalTypeCode, startDate, endDate, countType.getCode(), initializeCapCountRequest.getEffectiveDate()));
		} 

		clickEvent = capEntity.getMacroEventDateRange();
		if (clickEvent != null) {
			countType = CountType.MACRO_EVENT;
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();			
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertInitReqStmt.add(upsertInitCapEntityReqStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), 
				intervalTypeCode, startDate, endDate, countType.getCode(), initializeCapCountRequest.getEffectiveDate()));
		} 		

		clickEvent = capEntity.getGlobalEventDateRange();
		if (clickEvent != null) {
			countType = CountType.GLOBAL;			
			startDate = clickEvent.getStartDate();
			endDate = clickEvent.getEndDate();
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertInitReqStmt.add(upsertInitCapEntityReqStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), 
				intervalTypeCode, startDate, endDate, countType.getCode(), initializeCapCountRequest.getEffectiveDate()));
		} 				
		
		eventReqInfo = capEntity.getEventReqInfo();
		if (eventReqInfo != null) {
			countType = CountType.MICRO_EVENT;
			startDate = eventReqInfo.getStartDate();
			endDate = eventReqInfo.getEndDate();
			if (endDate == null) {
				endDate = startDate;
			} 			
			batchUpsertInitReqStmt.add(upsertInitMicroEventReqStmt.bind(incrementCount, entityTypeCode, capEntity.getId(), 
				intervalTypeCode, startDate, endDate, initializeCapCountRequest.getEffectiveDate()));
		}
		
		if (countType == null) {
			// error no count type is specified. TODO handle error
			throw new RuntimeException ("error no count type is specified for request " + initializeCapCountRequest);
		}			
		*/	
	}
	
	public List<InitializeCapCountRequest> getOutsandingInitializeCapCountRequests () {
		List<InitializeCapCountRequest> initializeCapCountRequests;
		
		initializeCapCountRequests = new ArrayList<> ();
		
		
		return initializeCapCountRequests;
	}
}
