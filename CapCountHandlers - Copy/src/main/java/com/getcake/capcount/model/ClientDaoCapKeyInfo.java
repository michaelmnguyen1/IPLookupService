package com.getcake.capcount.model;

import java.io.Serializable;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.getcake.capcount.dao.MsSqlCapCountDao;

public class ClientDaoCapKeyInfo implements Serializable {
	private int clientId;
	private String countKeySpace, reqKeySpace;
	private PreparedStatement updateClickCountStmt, updateClickCountDateStmt; 
	private PreparedStatement updateMacroEventCountStmt, updateMacroEventCountDateStmt;
	
	private PreparedStatement selectMultiMicroEventCountStmt, updateMicroEventCountStmt, updateMicroEventCountDateStmt;
	private PreparedStatement updateGlobalEventCountStmt, updateGlobalEventCountDateStmt;
	
	private PreparedStatement deleteCapEntityCountStmt, deleteCapEntityDateStmt, deleteMicroEventCountStmt, deleteMicroEventDateStmt;
	
	private PreparedStatement insertReqStmt, deleteReqStmt; 
	
	private PreparedStatement selectInitCapEntityReqStmt, upsertInitCapEntityReqStmt, upsertInitCapEntityReqStmt_Sec_Region, 
		deleteInitCapEntityReqStmt, selectPrevInitCapEntityReqStmt;
	private PreparedStatement selectInitMicroEventReqStmt, upsertInitMicroEventReqStmt, deleteInitMicroEventReqStmt;

	private PreparedStatement selectCapEntityCountsStmt, selectMicroEventCountStmt;
	
	private MsSqlCapCountDao primarySqlServer, standbySqlServer;	
	
	public void setClientId (int clientId) {
		this.clientId = clientId;
	}
	
	public int getClientId () {
		return this.clientId;
	}
	
	public void setCountKeySpace (String keySpace) {
		this.countKeySpace = keySpace;
	}
	
	public String getCountKeySpace () {
		return this.countKeySpace;
	}
		
	public void setRequestKeySpace (String reqKeySpace) {
		this.reqKeySpace = reqKeySpace;
	}
	
	public String getRequestKeySpace () {
		return this.reqKeySpace;
	}
		
	public void setInsertReqStmt (PreparedStatement insertReqStmt) {
		this.insertReqStmt = insertReqStmt;
	}
	
	public PreparedStatement getInsertReqStmt () {
		return this.insertReqStmt;
	}

		
	public void setSelectInitCapEntityReqStmt (PreparedStatement selectInitCapEntityReqStmt) {
		this.selectInitCapEntityReqStmt = selectInitCapEntityReqStmt;
	}
	
	public PreparedStatement getSelectInitCapEntityReqStmt () {
		return this.selectInitCapEntityReqStmt;
	}

	public void setUpsertInitCapEntityReqStmt (PreparedStatement insertInitializeReqStmt) {
		this.upsertInitCapEntityReqStmt = insertInitializeReqStmt;
	}
	
	public PreparedStatement getUpsertInitCapEntityReqStmt () {
		return this.upsertInitCapEntityReqStmt;
	}

	public void setupsertInitCapEntityReqStmt_Sec_Region (PreparedStatement upsertInitCapEntityReqStmt_Sec_Region) {
		this.upsertInitCapEntityReqStmt_Sec_Region = upsertInitCapEntityReqStmt_Sec_Region;
	}
	
	public PreparedStatement getupsertInitCapEntityReqStmt_Sec_Region () {
		return this.upsertInitCapEntityReqStmt_Sec_Region;
	}
		
	public void setDeleteInitCapEntityReqStmt (PreparedStatement deleteInitializeReqStmt) {
		this.deleteInitCapEntityReqStmt = deleteInitializeReqStmt;
	}
	
	public PreparedStatement getDeleteInitCapEntityReqStmt () {
		return this.deleteInitCapEntityReqStmt;
	}

	public void setSelectPendingInitCapEntityReqStmt (PreparedStatement selectPrevInitCapEntityReqStmt) {
		this.selectPrevInitCapEntityReqStmt = selectPrevInitCapEntityReqStmt;
	}
	
	public PreparedStatement getSelectPendingInitCapEntityReqStmt () {
		return this.selectPrevInitCapEntityReqStmt;
	}

	// micro	
	public void setSelectInitMicroEventReqStmt (PreparedStatement selectInitMicroEventReqStmt) {
		this.selectInitMicroEventReqStmt = selectInitMicroEventReqStmt;
	}
	
	public PreparedStatement getSelectInitMicroEventReqStmt () {
		return this.selectInitMicroEventReqStmt;
	}
					
	public void setUpsertInitMicroEventReqStmt (PreparedStatement upsertInitMicroEventReqStmt) {
		this.upsertInitMicroEventReqStmt = upsertInitMicroEventReqStmt;
	}
	
	public PreparedStatement getUpsertInitMicroEventReqStmt () {
		return this.upsertInitMicroEventReqStmt;
	}
					
	public void setDeleteInitMicroEventReqStmt (PreparedStatement deleteInitMicroEventReqStmt) {
		this.deleteInitMicroEventReqStmt = deleteInitMicroEventReqStmt;
	}
	
	public PreparedStatement getDeleteInitMicroEventReqStmt () {
		return this.deleteInitMicroEventReqStmt;
	}
					
	
	public void setDeleteReqStmt (PreparedStatement deleteReqStmt) {
		this.deleteReqStmt = deleteReqStmt;
	}
	
	public PreparedStatement getDeleteReqStmt () {
		return this.deleteReqStmt;
	}
	
	public void setUpdateClickCountStmt (PreparedStatement updateClickCountStmt) {
		this.updateClickCountStmt = updateClickCountStmt;
	}
	
	public PreparedStatement getUpdateClickCountStmt () {
		return this.updateClickCountStmt;
	}
	
	public void setUpdateClickCountDateStmt (PreparedStatement updateClickCountDateStmt) {
		this.updateClickCountDateStmt = updateClickCountDateStmt;
	}
	
	public PreparedStatement getUpdateClickCountDateStmt () {
		return this.updateClickCountDateStmt;
	}
	
	// Macro
	public void setUpdateMacroEventCountStmt (PreparedStatement updateMacroEventCountStmt) {
		this.updateMacroEventCountStmt = updateMacroEventCountStmt;
	}
	
	public PreparedStatement getUpdateMacroEventCountStmt () {
		return this.updateMacroEventCountStmt;
	}
	
	public void setUpdateMacroEventCountDateStmt (PreparedStatement updateMacroEventCountDateStmt) {
		this.updateMacroEventCountDateStmt = updateMacroEventCountDateStmt;
	}
	
	public PreparedStatement getUpdateMacroEventCountDateStmt () {
		return this.updateMacroEventCountDateStmt;
	}
	
	// Micro event	
	public void setSelectMultiMicroEventCountStmt (PreparedStatement selectMultiMicroEventCountStmt) {
		this.selectMultiMicroEventCountStmt = selectMultiMicroEventCountStmt;
	}
	
	public PreparedStatement getSelectMultiMicroEventCountStmt () {
		return this.selectMultiMicroEventCountStmt;
	}
		
	public void setUpdateMicroEventCountStmt (PreparedStatement updateMicroEventCountStmt) {
		this.updateMicroEventCountStmt = updateMicroEventCountStmt;
	}
	
	public PreparedStatement getUpdateMicroEventCountStmt () {
		return this.updateMicroEventCountStmt;
	}
	
	public void setUpdateMicroEventCountDateStmt (PreparedStatement updateMicroEventCountDateStmt) {
		this.updateMicroEventCountDateStmt = updateMicroEventCountDateStmt;
	}
	
	public PreparedStatement getUpdateMicroEventCountDateStmt () {
		return this.updateMicroEventCountDateStmt;
	}
	
	// Global Event
	public void setUpdateGlobalEventCountStmt (PreparedStatement updateGlobalEventCountStmt) {
		this.updateGlobalEventCountStmt = updateGlobalEventCountStmt;
	}
	
	public PreparedStatement getUpdateGlobalEventCountStmt () {
		return this.updateGlobalEventCountStmt;
	}
	
	public void setUpdateGlobalEventCountDateStmt (PreparedStatement updateGlobalEventCountDateStmt) {
		this.updateGlobalEventCountDateStmt = updateGlobalEventCountDateStmt;
	}
	
	public PreparedStatement getUpdateGlobalEventCountDateStmt () {
		return this.updateGlobalEventCountDateStmt;
	}
		
	public void setDeleteCapEntityCountStmt (PreparedStatement deleteCapEntityCountStmt) {
		this.deleteCapEntityCountStmt = deleteCapEntityCountStmt;
	}
	
	public PreparedStatement getDeleteCapEntityCountStmt () {
		return this.deleteCapEntityCountStmt;
	}
	
	
	public void setDeleteCapEntityDateStmt (PreparedStatement deleteCapEntityDateStmt) {
		this.deleteCapEntityDateStmt = deleteCapEntityDateStmt;
	}
	
	public PreparedStatement getDeleteCapEntityDateStmt () {
		return this.deleteCapEntityDateStmt;
	}
	
	
	public void setDeleteMicroEventCountStmt (PreparedStatement deleteMicroEventCountStmt) {
		this.deleteMicroEventCountStmt = deleteMicroEventCountStmt;
	}
	
	public PreparedStatement getDeleteMicroEventCountStmt () {
		return this.deleteMicroEventCountStmt;
	}
	
	
	public void setDeleteMicroEventDateStmt (PreparedStatement deleteMicroEventDateStmt) {
		this.deleteMicroEventDateStmt = deleteMicroEventDateStmt;
	}
	
	public PreparedStatement getDeleteMicroEventDateStmt () {
		return this.deleteMicroEventDateStmt;
	}
		
	public void setSelectCapEntityCountsStmt (PreparedStatement selectCapEntityCountsStmt) {
		this.selectCapEntityCountsStmt = selectCapEntityCountsStmt;
	}
	
	public PreparedStatement getSelectCapEntityCountsStmt () {
		return this.selectCapEntityCountsStmt;
	}

	public void setSelectMicroEventCountStmt (PreparedStatement selectMicroEventCountStmt) {
		this.selectMicroEventCountStmt = selectMicroEventCountStmt;
	}
	
	public PreparedStatement getSelectMicroEventCountStmt () {
		return this.selectMicroEventCountStmt;
	}

	public void setPrimarySqlServer (MsSqlCapCountDao primarySqlServer) {
		this.primarySqlServer = primarySqlServer;
	}
	
	public MsSqlCapCountDao getPrimarySqlServer () {
		return this.primarySqlServer;
	}
	
	public void setStandbySqlServer (MsSqlCapCountDao standbySqlServer) {
		this.standbySqlServer = standbySqlServer;
	}
	
	public MsSqlCapCountDao getStandbySqlServer () {
		return this.standbySqlServer;
	}
	
	
}
