package com.getcake.capcount.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CapEntityMicroEvents extends CapEntity {
	
	@JsonProperty("events")
	private List<MicroEvent> microEvents;
	
	public void setMicroEvents (List<MicroEvent> eventReqInfoList) {
		this.microEvents = eventReqInfoList;
	}
	
	public List<MicroEvent> getMicroEvents () {
		return this.microEvents;
	}	
}
