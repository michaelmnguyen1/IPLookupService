/*
 * Michael M. Nguyen
 */
package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeoISPResponse {

	@JsonProperty("ip_address")
	public String ipAddress;

	@JsonProperty("http_status_code")
	public String httpResponseCode;

	@JsonProperty("geo_isp")
	public GeoISP geoISP;

}
