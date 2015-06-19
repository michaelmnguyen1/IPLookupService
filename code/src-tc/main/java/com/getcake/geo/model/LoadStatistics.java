package com.getcake.geo.model;

public class LoadStatistics {
	public long count;
	public double accDuration, maxDuration = 0, minDuration = Double.MAX_VALUE;
	public double avgDurationNanoSec, avgDurationMicroSec, avgDurationMilliSec;
}
