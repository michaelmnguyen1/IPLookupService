/*
 * Michael M. Nguyen
 */
package com.getcake.geo.model;

public class Ipv4RangeNode {

	private long [] startArray, endArray;
	private int [] locationIdArray;

	public void setStartArray (long [] startArray) {
		this.startArray = startArray;
	}

	public long []  getStartArray () {
		return this.startArray;
	}

	public void setEndArray (long [] endArray) {
		this.endArray = endArray;
	}

	public long []  getEndArray () {
		return this.endArray;
	}

	public void setLocationIdArray (int [] locationIdArray) {
		this.locationIdArray = locationIdArray;
	}

	public int []  getLocationIdArray () {
		return this.locationIdArray;
	}

}
