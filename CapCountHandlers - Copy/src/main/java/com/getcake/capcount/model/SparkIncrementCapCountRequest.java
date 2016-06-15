package com.getcake.capcount.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.streaming.Time;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "batch_time", "batch_uuid", "request_uuids", "increment_count", "client_id", "partition_id" })
public class SparkIncrementCapCountRequest extends IncrementCapCountRequest {
	
	@JsonProperty("batch_uuid")
	protected String batchUuid;
	
	@JsonProperty("request_uuids")
	protected List<String> requestUuids;
	
	@JsonProperty("partition_id")
	protected int partitionId;
	
	@JsonProperty("batch_time")
	protected Time batchTime;
	
	public SparkIncrementCapCountRequest () {
		requestUuids = new ArrayList<String> ();
	}
	
	public void setBatchUuid (String batchtUuid) {
	  this.batchUuid = batchtUuid;
	}

	public String getBatchUuid () {
	  return this.batchUuid;
	}

	public void addMergedRequestUuids (String requestUuid) {
	  requestUuids.add(requestUuid);
	}

	public void setMergedRequestUuids (List<String> requestUuids) {
	  this.requestUuids = requestUuids;
	}

	public List<String> getMergedRequestUuids () {
	  return this.requestUuids;
	}

	public void setPartitionId (int partitionId) {
	  this.partitionId = partitionId;
	}

	public int getPartitionId () {
	  return this.partitionId;
	}

	public void setBatchTime (Time batchTime) {
	  this.batchTime = batchTime;
	}

	public Time getBatchTime () {
	  return this.batchTime;
	}

}
