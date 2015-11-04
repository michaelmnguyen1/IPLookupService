package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class GeoInfo {
    private int location_id;
	private float latitude, longitude;	 
	private String city, region, country, metro_code;
	
	@JsonProperty("location_id")
    public void setLocationId (int id) {
        this.location_id = id;
    }

	@JsonProperty("location_id")
    public int getLocationId() {
        return location_id;
    }

	public void setLatitude (float latitude) {
		this.latitude = latitude;
	}
	
	public float getLatitude () {
		return this.latitude;
	}
	
	public void setLongitude (float longitude) {
		this.longitude = longitude;
	}
	
	public float getLongitude () {
		return this.longitude;
	}
	
	public void setCity (String city) {
		this.city = city;
	}
	
	public String getCity () {
		return this.city;
	}
	
	public void setRegion (String region) {
		this.region = region;
	}
	
	public String getRegion () {
		return this.region;
	}
	
	public void setCountry (String country) {
		this.country = country;
	}
	
	public String getCountry () {
		return this.country;
	}

	@JsonProperty("metro_code")
	public void setMetroCode (String metroCode) {
		this.metro_code = metroCode;
	}
	
	@JsonProperty("metro_code")
	public String getMetroCode () {
		return this.metro_code;
	}

}
