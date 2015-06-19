package com.getcake.geo.model;

public class IpData {
	int id;
    private byte [] ipv6_start, ipv6_end;
    int location_id;
    
    public void setId (int id) {
    	this.id = id;
    }
    
    public int getId () {
    	return this.id;
    }
    
    public void setIpv6_start (byte [] ipv6_start) {
    	this.ipv6_start = ipv6_start;
    }
    
    public byte [] getIpv6_start () {
    	return this.ipv6_start;
    }
    
    public void setIpv6_end (byte [] ipv6_end) {
    	this.ipv6_end = ipv6_end;
    }
    
    public byte [] getIpv6_end () {
    	return this.ipv6_end;
    }
    
    public void setLocation_id (int location_id) {
    	this.location_id = location_id;
    }
    
    public int getLocation_id () {
    	return this.location_id;
    }
    
}
