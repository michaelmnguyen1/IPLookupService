/*
 * Michael M. Nguyen
 */
package com.getcake.geo.model;

import java.util.HashMap;

public class ISPCache {

	public HashMap<Long, ISPRangeNode> subBytesCache;
	public HashMap<Long, Integer> fullBytesCache;

	public long maxSubRange = 0, minSubRange = Long.MAX_VALUE, maxNodeLength = 0, minNodeLength = Long.MAX_VALUE;
	public String maxNodeIpStart, maxNodeIpEnd;
	public String minNodeIpStart, minNodeIpEnd;
	public long numNodes = 0;

}
