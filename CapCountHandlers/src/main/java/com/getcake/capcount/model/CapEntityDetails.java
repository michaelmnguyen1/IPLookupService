package com.getcake.capcount.model;

public class CapEntityDetails {

	private CapEntity capEntity;
	private ClickEvent clickEvent;
	private	CountType countType;

	public void setCapEntity (CapEntity capEntity) {
		this.capEntity = capEntity;
	}

	public CapEntity getCapEntity () {
		return this.capEntity;
	}

	public void setClickEvent (ClickEvent clickEvent) {
		this.clickEvent = clickEvent;
	}

	public ClickEvent getClickEvent () {
		return this.clickEvent;
	}

	public void setCountType (CountType countType) {
		this.countType = countType;
	}

	public CountType getCountType () {
		return this.countType;
	}
	
}
