package com.getcake.capcount.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClickEventCounts {

	private long clickCount, macroEventCount, globalEventCount;
	private Date startDate;	  
	private Date endDate;

	
	public void setClickCount (long clickCount) {
		this.clickCount = clickCount;
	}
	
	public long getClickCount () {
		return this.clickCount;
	}
	
	public void setMacroEventCount (long macroEventCount) {
		this.macroEventCount = macroEventCount;
	}
	
	public long getMacroEventCount () {
		return this.macroEventCount;
	}

	public void setGlobalEventCount (long globalEventCount) {
		this.globalEventCount = globalEventCount;
	}
	
	public long getGlobalEventCount () {
		return this.globalEventCount;
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
