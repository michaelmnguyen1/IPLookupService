package com.getcake.geo.model;

public class GeoInfo {
    private int locationId, importId;
	private float latitude, longitude;	 
	private String city, region, country, metroCode;
	
    public void setLocationId (int id) {
        this.locationId = id;
    }

    public int getLocationId() {
        return locationId;
    }

    public void setImportId (int id) {
        this.importId = id;
    }

    public int getImportId() {
        return importId;
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

	public void setMetroCode (String metroCode) {
		this.metroCode = metroCode;
	}
	
	public String getMetroCode () {
		return this.metroCode;
	}

}
