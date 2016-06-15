package com.getcake.capcount.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class InitializeCapCountRequest extends SparkIncrementCapCountRequest {
	
	private static final long serialVersionUID = 1L;
	
	@JsonProperty("effective_date")
	protected Date effectiveDate;

	public void setEffectiveDate (Date effectiveDate) {
	  this.effectiveDate = effectiveDate;
	}

	public Date getEffectiveDate () {
	  return this.effectiveDate;
	}
	
	public boolean equals (Object object) {
		
		if (! (object instanceof InitializeCapCountRequest)) {
			return false;
		}
		
		InitializeCapCountRequest target = (InitializeCapCountRequest)object;
		
		if (clientId != target.clientId) {
			return false;
		}
		
		if (batchUuid != target.batchUuid) {
			return false;
		}
		
		if (batchTime != null && (! batchTime.equals(target.batchTime))) {
			return false;
		} else if (target.batchTime != null && (! target.batchTime.equals(batchTime))) {
			return false;
		}
		
		if (! requestUuid.equalsIgnoreCase(target.requestUuid)) {
			return false;
		}
		
		if (effectiveDate != null && (! effectiveDate.equals(target.effectiveDate))) {
			return false;
		} else if (target.effectiveDate != null && (! target.effectiveDate.equals(effectiveDate))) {
			return false;
		}
		
		return true;
	}

	public int hashCode () {
		return toString ().hashCode();
	}
	
	public String toString () {
		return "client: " + clientId + " - requestUuid: " + requestUuid + " - incrementCount: " + incrementCount + 
			" - requestDate: " + requestDate + " - batchUuid: " + this.batchUuid + 
			" - partitionId: " + partitionId + " - batchTime:" + batchTime; 
	}		
}
