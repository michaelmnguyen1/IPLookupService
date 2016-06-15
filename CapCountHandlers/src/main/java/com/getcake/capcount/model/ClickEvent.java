package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClickEvent extends ClickEventTypeKey {
	 
	protected long count;

	public void setCount (long count) {
		this.count = count;
	}
	
	public long getCount () {
		return this.count;
	}
	
}
