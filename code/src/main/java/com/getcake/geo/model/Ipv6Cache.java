package com.getcake.geo.model;

import java.math.BigInteger;
import java.util.HashMap;

public class Ipv6Cache {

	public HashMap<BigInteger, Ipv6RangeNode> subBytesCache;
	
	public long maxSubRange = 0, minSubRange = Long.MAX_VALUE, maxNodeLength = 0, minNodeLength = Long.MAX_VALUE;
	public String maxNodeIpStart, maxNodeIpEnd;
	public String minNodeIpStart, minNodeIpEnd;
	public long numNodes = 0;
	
}
