package com.getcake.capcount.model;

import java.util.ArrayList;
import java.util.List;

public class ChainStreamCapCountRequest {
  private List<StreamCapCountRequest> capCountRequests;
  
  public ChainStreamCapCountRequest () {
	  capCountRequests = new ArrayList<> ();
  }
  
  public void setCapCountRequests (List<StreamCapCountRequest> capCountRequests) {
	  this.capCountRequests = capCountRequests;	  
  }
  
  public void addCapCountRequests (StreamCapCountRequest capCountRequest) {
	  this.capCountRequests.add (capCountRequest);	  
  }
  
  public List<StreamCapCountRequest> getCapCountRequests () {
	  return this.capCountRequests;	  
  }
  
  
}
