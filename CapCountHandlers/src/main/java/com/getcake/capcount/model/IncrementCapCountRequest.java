package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class IncrementCapCountRequest implements Serializable {
		
	protected static final long serialVersionUID = 1L;
	
	@JsonProperty("request_uuid")
	protected String requestUuid;
	
	@JsonProperty("client_id")
	protected int clientId;
	
	@JsonProperty("increment_count")
	protected long incrementCount;

	@JsonProperty("request_date")
	protected Date requestDate;

	@JsonProperty("offer_contract")
	protected CapEntity offerContract;

	@JsonProperty("offer")
	protected CapEntity offer;

	@JsonProperty("campaign")
	protected CapEntity campaign;

	@JsonProperty("global_event")
	protected CapEntity globalEventReqInfo;

	public void setRequestUuid (String requestUuid) {
	  this.requestUuid = requestUuid;
	}

	public String getRequestUuid () {
	  return this.requestUuid;
	}

	public void setIncrementCount (long incrementCount) {
	  this.incrementCount = incrementCount;
	}

	public long getIncrementCount () {
	  return this.incrementCount;
	}

	public void setClientId (int clientId) {
	  this.clientId = clientId;
	}

	public int getClientId () {
	  return this.clientId;
	}	
	
	public void setRequestDate (Date requestDate) {
	  this.requestDate = requestDate;
	}

	public Date getRequestDate () {
	  return this.requestDate;
	}

	public void setOfferContract (CapEntity offerContract) {
	  this.offerContract = offerContract;
	  if (offerContract != null) {
		  this.offerContract.setCapEntityType(CapEntityType.OfferContract);
	  }
	}

	public CapEntity getOfferContract () {
	  return this.offerContract;
	}

	public void setOffer (CapEntity offer) {
	  this.offer = offer;
	  if (offer != null) {
		  this.offer.setCapEntityType(CapEntityType.Offer);
	  }
	}

	public CapEntity getOffer () {
	  return this.offer;
	}

	public void setCampaign (CapEntity campaign) {
	  this.campaign = campaign;
	  if (campaign != null) {
		  this.campaign.setCapEntityType(CapEntityType.Campaign);		  
	  }
	}

	public CapEntity getCampaign () {
	  return this.campaign;
	}

	public void setGlobalEventReqInfo (CapEntity globalEventReqInfo) {
	  this.globalEventReqInfo = globalEventReqInfo;
	  if (globalEventReqInfo != null) {
		  this.globalEventReqInfo.setCapEntityType(CapEntityType.Global);
	  }
	}

	public CapEntity getGlobalEventReqInfo () {
	  return this.globalEventReqInfo;
	}

	
}
