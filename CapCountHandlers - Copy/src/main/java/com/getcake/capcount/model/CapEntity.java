package com.getcake.capcount.model;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CapEntity  implements Serializable {
  private int id;
  
  private CapEntityType capEntityType;
  
	@JsonProperty("click")
	private ClickEvent click;

	@JsonProperty("macro_event")
	private ClickEvent macroEvent;

	@JsonProperty("global_event")
	private ClickEvent globalEvent;

	@JsonProperty("event")
	private MicroEvent microEvent;

	public void setId (int id) {
	  this.id = id;
	}

	public int getId () {
	  return this.id;
	}

	public void setCapEntityType (CapEntityType capEntityType) {
	  this.capEntityType = capEntityType;
	}

	public CapEntityType getCapEntityType () {
	  return this.capEntityType;
	}

	public void setClick (ClickEvent click) {
	  this.click = click;
	}

	public ClickEvent getClick () {
	  return this.click;
	}

	public void setMacroEvent (ClickEvent macroEvent) {
	  this.macroEvent = macroEvent;
	}

	public ClickEvent getMacroEvent () {
	  return this.macroEvent;
	}

	public void setMicroEvent (MicroEvent microEvent) {
	  this.microEvent = microEvent;
	}

	public MicroEvent getMicroEvent () {
	  return this.microEvent;
	}

	public void setGlobalEvent (ClickEvent globalEvent) {
	  this.globalEvent = globalEvent;
	}

	public ClickEvent getGlobalEvent () {
	  return this.globalEvent;
	}

	public String toString () {
	  StringBuilder strBuilder = new StringBuilder ();
	  strBuilder.append("CapEntity id:" + id + " - capEntityType:" + capEntityType);
	  if (click != null) {
		  strBuilder.append(" - clickDateRange: " + click.toString());
	  }
	  if (macroEvent != null) {
		  strBuilder.append(" - macroEventDateRange: " + macroEvent.toString());
	  }
	  if (globalEvent != null) {
		  strBuilder.append(" - globalEventDateRange: " + globalEvent.toString());
	  }
	  if (microEvent != null) {
		  strBuilder.append(" - eventReqInfo: " + microEvent.toString());
	  }
	  return strBuilder.toString();
	}
}
