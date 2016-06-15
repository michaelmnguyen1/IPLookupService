package com.getcake.capcount.model;

public enum RequestType {
	INC_CAP_COUNT ("IC", "Increment Cap Count"), DEL_CAP_COUNT ("DC", "Delete Cap Count");
	  
  	private String code, name;
	private RequestType (String code, String name) {
		this.code = code;
		this.name = name;
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

}
