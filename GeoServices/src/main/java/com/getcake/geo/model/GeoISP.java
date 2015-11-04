package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeoISP {
	
	@JsonProperty("ip_address")
	public String ipAddress;
	
	@JsonProperty("http_status_code")
	public String httpResponseCode;
	
	@JsonProperty("geo_info")
	public GeoInfo geoInfo;
	
	@JsonProperty("isp_info")
	public IspInfo ispInfo;
}
