package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.getcake.capcount.services.CapCountStreamHandler;

public class CapCountReqKey implements Serializable {
	
	private static final Logger logger = Logger.getLogger(CapCountReqKey.class);

	private String batchUuid;
	protected List<String> requestUuids;
	
	private int clientId, entityId, eventId, partitionId;
	private long batchTime;
	private CapEntityType entityType;
	private CountType countType;
	private IntervalType intervalType;
	private Date startDate, endDate;
	private boolean deleteReq;
	private long accIncrementCount;
	private List<StreamCapCountRequest> capCountRequests;
	private List<String> capCountRequestStrings;
	
	public CapCountReqKey () {
		capCountRequests = new ArrayList<>();
		capCountRequestStrings = new ArrayList<>();
		requestUuids = new ArrayList<>();
	}
	
	public int hashCode () {
		return toString ().hashCode();
	}
	
	public boolean equals (Object object) {
		
		if (! (object instanceof CapCountReqKey)) {
			return false;
		}
		
		CapCountReqKey target = (CapCountReqKey)object;
		
		if (clientId != target.clientId) {
			return false;
		}
		
		if (batchUuid != target.batchUuid) {
			return false;
		}
		
		if (entityType  != target.entityType) {
			return false;
		}
		
		if (entityId != target.entityId) {
			return false;
		}
		
		if (countType  != target.countType) {
			return false;
		}
		
		if (intervalType  != target.intervalType) {
			return false;
		}
		
		if (eventId != target.eventId) {
			return false;
		}
		
		if (! startDate.equals(target.startDate)) {
			return false;
		}
		
		if (! endDate.equals(target.endDate)) {
			return false;
		}
		
		logger.debug("CapCountKey equals - this.entityId: " + entityId + " - this.accIncrementCount: " + this.accIncrementCount 
			+ " - target.entityId: " + target.entityId + " - target.accIncrementCount: " + target.accIncrementCount 
			+ " - this.countType: " + this.countType + " - this.intervalType: " + target.intervalType
			+ " - target.intervalType: " + target.intervalType + " - target.intervalType: " + target.intervalType);
		
		return true;
	}	
	
	public String toString () {
		return "client: " + clientId + " - batchUuid: " + batchUuid + " - partitionId: " + partitionId + " - batchTime:" + batchTime + 
			" - entityId: " + entityId + " - eventId: " + eventId + " - entityTypeCode: " + entityType + " - countTypeCode: " + countType + 
			" - intervalTypeCode: " + intervalType + " - startDate: " + startDate + " - endDate: " + endDate; 		
	}
	
	public List<String> getRequestUuids () {
		return requestUuids;
	}
	
	public void mergeCapCountKey (CapCountReqKey target) {		
		List<StreamCapCountRequest> capCountRequests = target.capCountRequests;
		for (StreamCapCountRequest  capCountRequest : capCountRequests) {
			requestUuids.add(capCountRequest.getIncrementCapCountReq().getRequestUuid());
		} 
		
		this.capCountRequests.addAll(target.capCountRequests);
		
		this.accIncrementCount += target.accIncrementCount;
		
		this.capCountRequestStrings.addAll(target.capCountRequestStrings);
	}
	
	public void setBatchUuid (String batchUuid) {
		this.batchUuid = batchUuid;
	}
	
	public String getBatchUuid () {
		return this.batchUuid;
	}
	
	public void setPartitionId (int partitionId) {
		this.partitionId = partitionId;
	}
	
	public int getPartitionId () {
		return this.partitionId;
	}
	
	public void setBatchTime (long batchTime) {
		this.batchTime = batchTime;
	}
	
	public long getBatchTime () {
		return this.batchTime;
	}
	
	public void setDeleteReq (boolean deleteReq) {
		this.deleteReq = deleteReq;
	}
	
	public boolean isDeleteReq () {
		return this.deleteReq;
	}
	
	public void addCapCountRequest (StreamCapCountRequest newCapCountRequest, String capCountRequestString) {
		StreamCapCountRequest capCountRequest;
		
		requestUuids.add(newCapCountRequest.getIncrementCapCountReq().getRequestUuid());
		this.capCountRequests.add(newCapCountRequest);
		this.capCountRequestStrings.add(capCountRequestString);
		
		if (newCapCountRequest.getIncrementCapCountReq() != null) {
			this.accIncrementCount += newCapCountRequest.getIncrementCapCountReq().getIncrementCount();			
		} else {
			InitializeCapCountRequest initializeCapCountReq = newCapCountRequest.getInitializeCapCountReq();
			Date deleteReqDate = initializeCapCountReq.getRequestDate();
			for (int i = capCountRequests.size(); i >= 0; i--) {
				capCountRequest = capCountRequests.get(i);
				if (capCountRequest.getIncrementCapCountReq().getRequestDate().before(deleteReqDate)) {
					this.accIncrementCount -= capCountRequest.getIncrementCapCountReq().getIncrementCount();			
					capCountRequests.remove(i);
				}
			}
			this.deleteReq = true;			
		}
	}
	
	public List<StreamCapCountRequest> getCapCountRequests () {
		return this.capCountRequests;
	}

	public void addAccIncrementCount (long accIncrementCount) {
		this.accIncrementCount += accIncrementCount;
	}
	
	public void setAccIncrementCount (long accIncrementCount) {
		this.accIncrementCount = accIncrementCount;
	}
	
	public long getAccIncrementCount () {
		return this.accIncrementCount;
	}

	public void setClientId (int clientId) {
		this.clientId = clientId;
	}
	
	public int getClientId () {
		return this.clientId;
	}

	public void setEntityId (int entityId) {
		this.entityId = entityId;
	}
	
	public int getEntityId () {
		return this.entityId;
	}
	
	public void setEventId (int eventId) {
		this.eventId = eventId;
	}
	
	public int getEventId () {
		return this.eventId;
	}
	
	public void setEntityType (CapEntityType entityTypeCode) {
		this.entityType = entityTypeCode;
	}
	
	public CapEntityType getEntityType () {
		return this.entityType;
	}
	
	public void setCountType (CountType countType) {
		this.countType = countType;
	}
	
	public CountType getCountType () {
		return this.countType;
	}
	
	public void setIntervalType (IntervalType intervalType) {
		this.intervalType = intervalType;
	}
	
	public IntervalType getIntervalType () {
		return this.intervalType;
	}		
	
	public void setStartDate (Date startDate) {
		this.startDate = startDate;
	}
	
	public Date getStartDate () {
		return this.startDate;
	}
	
	public void setEndDate (Date endDate) {
		this.endDate = endDate;
	}
	
	public Date getEndDate () {
		return this.endDate;
	}		
}
