package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class LoadStatistics {
	public long count;
	
	@JsonIgnore
	public double allAlgorthmDurationNanoSec;
	
	@JsonIgnore
	public double allIpFormatConvDurationNanoSec;
	
	public double avgAlgorthmDurationMicroSec, avgIpFormatConvDurationMicroSec;
}
