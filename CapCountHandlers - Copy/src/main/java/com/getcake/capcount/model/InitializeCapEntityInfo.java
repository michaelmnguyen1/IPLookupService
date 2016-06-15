package com.getcake.capcount.model;

import java.util.Date;

public class InitializeCapEntityInfo extends CapEntity {

	private Date effectiveDate;

	public void setEffectiveDate (Date effectiveDate) {
	  this.effectiveDate = effectiveDate;
	}

	public Date getEffectiveDate () {
	  return this.effectiveDate;
	}
	
}
