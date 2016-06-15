package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClickEventTypeKey implements Serializable {

	static public final Date FIRST_DATE = Calendar.getInstance().getTime();
	
	static {
		FIRST_DATE.setTime(0);
	}

	@JsonProperty("interval_type")
	protected IntervalType intervalType;
	
	@JsonProperty("start_date")
	protected Date startDate;

	@JsonProperty("end_date")
	protected Date endDate;
  
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
		if (startDate == null) {
			return FIRST_DATE;
		}
	  return this.startDate;
	}

	public void setEndDate (Date endDate) {
	  this.endDate = endDate;
	}

	public Date getEndDate () {
	  // To do 
	  if (endDate == null) {
		  // endDate = new Date (); // Calendar.getInstance().getTime();
		  // endDate.setMonth(startDate.getMonth() + 1);
		  endDate = startDate;
		  if (endDate == null) {
			  return FIRST_DATE;
		  }
	  } 
	  return this.endDate;
	}
  
	public String toString () {
		return "intervalTypeCode: " + intervalType + " - startDate: " + startDate + " - endDate: " + endDate; 		
	}
			
	public int hashCode () {
		return toString ().hashCode();
	}
	
	public boolean equals (Object object) {
		if (! (object instanceof ClickEventTypeKey)) {
			return false;
		}
		
		ClickEventTypeKey target = (ClickEventTypeKey)object;
		
		if (intervalType != target.intervalType) {
			return false;
		}
		
		if (startDate != target.startDate && (! startDate.equals(target.startDate))) {
			return false;
		}
		
		if (endDate != target.endDate && (! endDate.equals(target.endDate))) {
			return false;
		}
		return true;		
	}

}
