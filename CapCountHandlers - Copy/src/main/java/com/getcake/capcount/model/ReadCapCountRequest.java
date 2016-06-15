package com.getcake.capcount.model;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ReadCapCountRequest implements Serializable {
		
	protected static final long serialVersionUID = 1L;
	
	@JsonProperty("client_id")
	protected int clientId;
	
	@JsonProperty("request_date")
	protected Date requestDate;

	@JsonProperty("offer_contract")
	protected CapEntityMicroEvents offerContract;

	@JsonProperty("offer")
	protected CapEntityMicroEvents offer;

	@JsonProperty("campaign")
	protected CapEntityMicroEvents campaign;

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

	public void setOfferContract (CapEntityMicroEvents offerContract) {
	  this.offerContract = offerContract;
	  if (offerContract != null) {
		  this.offerContract.setCapEntityType(CapEntityType.OfferContract);
	  }
	}

	public CapEntityMicroEvents getOfferContract () {
	  return this.offerContract;
	}

	public void setOffer (CapEntityMicroEvents offer) {
	  this.offer = offer;
	  if (offer != null) {
		  this.offer.setCapEntityType(CapEntityType.Offer);
	  }
	}

	public CapEntityMicroEvents getOffer () {
	  return this.offer;
	}

	public void setCampaign (CapEntityMicroEvents campaign) {
	  this.campaign = campaign;
	  if (campaign != null) {
		  this.campaign.setCapEntityType(CapEntityType.Campaign);		  
	  }
	}

	public CapEntityMicroEvents getCampaign () {
	  return this.campaign;
	}
}
