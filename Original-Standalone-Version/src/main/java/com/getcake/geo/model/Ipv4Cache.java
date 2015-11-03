package com.getcake.geo.model;

import java.util.HashMap;

public class Ipv4Cache {

	public HashMap<Long, Ipv4RangeNode> subBytesCache;
	public HashMap<Long, Integer> fullBytesCache;
	
	public long maxSubRange = 0, minSubRange = Long.MAX_VALUE, maxNodeLength = 0, minNodeLength = Long.MAX_VALUE;
	public String maxNodeIpStart, maxNodeIpEnd;
	public String minNodeIpStart, minNodeIpEnd;
	public long numNodes = 0;
	
}
