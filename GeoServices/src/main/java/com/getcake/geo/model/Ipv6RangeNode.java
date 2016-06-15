/*
 * Michael M. Nguyen
 */
package com.getcake.geo.model;

import java.math.BigInteger;

public class Ipv6RangeNode {

	private BigInteger [] startArray, endArray;
	private int [] locationIdArray;

	public void setStartArray (BigInteger [] startArray) {
		this.startArray = startArray;
	}

	public BigInteger []  getStartArray () {
		return this.startArray;
	}

	public void setEndArray (BigInteger [] endArray) {
		this.endArray = endArray;
	}

	public BigInteger []  getEndArray () {
		return this.endArray;
	}

	public void setLocationIdArray (int [] locationIdArray) {
		this.locationIdArray = locationIdArray;
	}

	public int []  getLocationIdArray () {
		return this.locationIdArray;
	}

}
