package com.getcake.geo.model;

public class MsSqlExportCheckResp {
	public String prevExportedIpVersion;
	public String newIpVersion;
	public String detailMsg;
	
	public String getPrevExportedIpVersion () {
		return prevExportedIpVersion;
	}
	
	public String getNewIpVersion () {
		return newIpVersion;
	}
	
	public String getDetailMsg () {
		return detailMsg;
	}
	
}
