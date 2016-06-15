/*
 * Michael M. Nguyen
 */
package com.getcake.geo.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LoadStatistics {
	public long count;

	@JsonIgnore
	public double allAlgorthmDurationNanoSec;

	@JsonIgnore
	public double allIpFormatConvDurationNanoSec;

	private double avgAlgorthmDurationMicroSec;

	@JsonProperty("average_lookup_micro_seconds")
	public void setAvgAlgorthmDurationMicroSec (double avgAlgorthmDurationMicroSec) {
		this.avgAlgorthmDurationMicroSec = avgAlgorthmDurationMicroSec;
	}

	@JsonProperty("average_lookup_micro_seconds")
	public double getAvgAlgorthmDurationMicroSec ( ) {
		return this.avgAlgorthmDurationMicroSec;
	}

	private double avgIpFormatConvDurationMicroSec;

	@JsonProperty("average_ip_convert_micro_seconds")
	public void setAvgIpFormatConvDurationMicroSec (double avgIpFormatConvDurationMicroSec) {
		this.avgIpFormatConvDurationMicroSec = avgIpFormatConvDurationMicroSec;
	}

	@JsonProperty("average_ip_convert_micro_seconds")
	public double getAvgIpFormatConvDurationMicroSec () {
		return this.avgIpFormatConvDurationMicroSec;
	}
}
