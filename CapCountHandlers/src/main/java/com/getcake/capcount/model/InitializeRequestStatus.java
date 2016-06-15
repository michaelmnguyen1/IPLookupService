package com.getcake.capcount.model;

import java.io.Serializable;

public enum InitializeRequestStatus implements Serializable {

	Ready ("RD", "Ready"), 
	Scheduled ("SC", "Scheduled"), 
	Scheduled_Retry ("SC_RT", "Scheduled_Retry"),
	Scheduled_Failed ("SC_FL", "Scheduled_Failed"),
	Scheduled_Failed_Max_Retries_Exceeded ("SC_FL_MX_RET", "Scheduled Failed Max Retries Exceeded"),
	Scheduled_Done ("SC_DN", "Scheduled_Done"), 

	Backfill ("BF", "Backfill"), 
	Backfill_Failed ("BF_FL", "Backfill_Failed"), 
	Backfill_Retry ("BF_RT", "Backfill_Retry"),	
	Backfill_Retry_Max_Retries_Exceeded ("BF_RT_MX_RET", "Backfill Retry Max Retries Exceeded"),	
	Backfill_Done ("BF_DN", "Backfill_Done"), 
	
	Increment_Done_Skipped_Scheduling ("INCD", "Increment Done Skipped Scheduling"), 

	Deleted_By_A_Later_Request ("DEL_LR", "Deleted By A Later Request"); 
	
  	private String code, name;
  	
	private InitializeRequestStatus (String code, String name) {
		this.code = code;
		this.name = name;
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
	
}
