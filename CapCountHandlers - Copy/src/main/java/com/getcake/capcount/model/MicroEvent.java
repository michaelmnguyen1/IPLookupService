package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MicroEvent extends ClickEvent {
	
	@JsonProperty("event_id")
	private int eventId;
	  
	public void setEventId (int eventId) {
		this.eventId = eventId;
	}
  
	public int getEventId () {
		return this.eventId;
	}

	public String toString () {
		return "intervalTypeCode: " + intervalType + " - startDate: " + startDate + " - endDate: " + endDate + " - eventId: " + eventId; 		
	}
			
	public int hashCode () {
		return toString ().hashCode();
	}
	
	public boolean equals (Object object) {
		if (! (object instanceof MicroEvent)) {
			return false;
		}
		
		MicroEvent target = (MicroEvent)object;
		
		if (intervalType != target.intervalType) {
			return false;
		}
		
		if (! startDate.equals(target.startDate)) {
			return false;
		}
		
		if (! endDate.equals(target.endDate)) {
			return false;
		}
		if (eventId != target.eventId) {
			return false;
		}
		return true;
		
	}

}
