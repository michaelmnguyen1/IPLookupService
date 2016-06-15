package com.getcake.capcount.model;

import java.io.Serializable;

public enum CountType implements Serializable {
	CLICK ("CK", "Click", 1), GLOBAL ("GB", "Global", 3), MICRO_EVENT ("EV", "Micro Event", 3), MACRO_EVENT ("MA", "Macro Event", 2);
	  
  	private String code, name;
  	private int dbId;
  	
	private CountType (String code, String name, int dbId) {
		this.code = code;
		this.name = name;
		this.dbId = dbId;
	}
	
	public String toString () {
		return code;
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
