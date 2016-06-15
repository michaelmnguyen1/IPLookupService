package com.getcake.capcount.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class StreamCapCountRequest implements Serializable {
	
	@JsonProperty("increment_cap_count_req")
	private IncrementCapCountRequest incrementCapCountReq;
	
	@JsonProperty("init_cap_count_req")
	private InitializeCapCountRequest initializeCapCountReq;  

	public void setIncrementCapCountReq (IncrementCapCountRequest incrementCapCountReq) {
	  this.incrementCapCountReq = incrementCapCountReq;
	}

	public IncrementCapCountRequest getIncrementCapCountReq () {
	  return this.incrementCapCountReq;
	}

	public void setInitializeCapCountReq (InitializeCapCountRequest initializeCapCountReq) {
	  this.initializeCapCountReq = initializeCapCountReq;
	}

	public InitializeCapCountRequest getInitializeCapCountReq () {
	  return this.initializeCapCountReq;
	}
}
