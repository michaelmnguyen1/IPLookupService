package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IspInfo {
	private String providerName;
	private int ispId;

	@JsonProperty("isp_provider_name")
	public void setProviderName (String providerName) {
		this.providerName = providerName;
	}
	
	@JsonProperty("isp_provider_name")
	public String getProviderName () {
		return this.providerName;
	}

	@JsonProperty("isp_id")
	public void setIspId (int ispId) {
		this.ispId = ispId;
	}
	
	@JsonProperty("isp_id")
	public int getIspId () {
		return this.ispId;
	}
	
	
}
