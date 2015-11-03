package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeoISP {
	
	@JsonProperty("geo_info")
	public GeoInfo geoInfo;
	
	@JsonProperty("isp_info")
	public IspInfo ispInfo;
}
