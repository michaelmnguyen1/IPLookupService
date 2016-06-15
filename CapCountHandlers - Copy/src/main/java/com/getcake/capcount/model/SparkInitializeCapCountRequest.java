package com.getcake.capcount.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SparkInitializeCapCountRequest extends InitializeCapCountRequest {

	@JsonProperty("initialize_request_status")
	private InitializeRequestStatus initializeRequestStatus;
	
	private int numRetries;
	private String requestReceivedRegion;	
	
	public void incrementNumRetries () {
		numRetries++;
	}
	
	public int getNumRetries () {
		return numRetries;
	}
	
	public void setInitializeRequestStatus (InitializeRequestStatus initializeRequestStatus) {
	  this.initializeRequestStatus = initializeRequestStatus;
	}

	public InitializeRequestStatus getInitializeRequestStatus () {
	  return this.initializeRequestStatus;
	}
	
	
	public void setRequestReceivedRegion (String requestReceivedRegion) {
		this.requestReceivedRegion = requestReceivedRegion;
	}
	
	public String getRequestReceivedRegion () {
		return this.requestReceivedRegion;
	}
	
}
