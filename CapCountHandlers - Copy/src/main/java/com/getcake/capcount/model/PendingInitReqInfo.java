package com.getcake.capcount.model;

import java.util.Date;

public class PendingInitReqInfo {

	public long initCount;
	public Date effectiveDate;	
	
	public void setInitCount (long initCount) {
		this.initCount = initCount;
	}
	
	public long getInitCount () {
		return this.initCount;
	}

	public void setEffectiveDate (Date effectiveDate) {
		this.effectiveDate = effectiveDate;
	}
	
	public Date getEffectiveDate () {
		return this.effectiveDate;
	}
	
	
}
