package com.getcake.capcount.model;

public class ClickCountInfo {
  private int id;
  private ClickEvent dateRange;
  
  public void setId (int id) {
	  this.id = id;
  }
  
  public int getId () {
	  return this.id;
  }

  public void setDateRange (ClickEvent dateRange) {
	  this.dateRange = dateRange;
  }
  
  public ClickEvent setDateRange () {
	  return this.dateRange;
  }
  
}
