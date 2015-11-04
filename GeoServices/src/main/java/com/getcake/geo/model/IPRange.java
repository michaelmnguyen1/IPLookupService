package com.getcake.geo.model;

public class IPRange {
  private String ipv6_start, ipv6_end;
  
  private int targetId;

    public String getIpv6_start() {
      return ipv6_start;
    }

    public void setIpv6_start(String ipv6_start_str) {	    
      this.ipv6_start = convertHextString (ipv6_start_str);
      // ipv6_start_bin = hexStringToByteArray (ipv6_start_str); 
    }

    private String convertHextString (String inputStr) {
		inputStr = inputStr.trim();
		if (inputStr.indexOf("0x") == 0) {
			inputStr = inputStr.substring(2, inputStr.length());
		}
		return inputStr;
    }

    public String getIpv6_end () {
      return ipv6_end;
    }

    public void setIpv6_end(String ipv6_end_str) {
	   this.ipv6_end = convertHextString (ipv6_end_str);
      // ipv6_end_bin = hexStringToByteArray (ipv6_end_str); 
    }
    
    public int getTargetId() {
      return targetId;
    }

    public void setTargetId(int targetId) {
      this.targetId = targetId;
    }
}

