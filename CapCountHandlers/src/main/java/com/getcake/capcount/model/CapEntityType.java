package com.getcake.capcount.model;

import java.io.Serializable;

public enum CapEntityType implements Serializable {
		
	OfferContract ("OC", "OfferContract", 14), Offer ("OF", "Offer", 5), Campaign ("CP", "Campaign", 6), Global ("GL", "Global", 18);
  
  	private String code, name;
  	private int dbId;
  	
	private CapEntityType (String code, String name, int dbId) {
		this.code = code;
		this.name = name;
		this.dbId = dbId;
	}
	
	public String toString () {
		return name;
	}

	public String getName () {
		return name;
	}
	
	public String getCode () {
		return code;
	}

	public int getDbId () {
		return dbId;
	}
	
}
