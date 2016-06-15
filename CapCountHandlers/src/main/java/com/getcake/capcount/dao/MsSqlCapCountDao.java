package com.getcake.capcount.dao;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

// import org.apache.calcite.avatica.SqlType;



import com.getcake.capcount.model.CapEntity;
import com.getcake.capcount.model.CapEntityDetails;
import com.getcake.capcount.model.ClickEvent;
import com.getcake.capcount.model.ClientDaoCapKeyInfo;
import com.getcake.dao.BaseDao;

public class MsSqlCapCountDao extends BaseDao implements Serializable {

	public int getClickEventCount (ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityDetails) throws Throwable {
		MsSqlCapCountDao sqlServerDBInfo = null;
		Connection mssqlConn = null;
		CallableStatement mssqlGetInitStoredProc = null;
		java.sql.ResultSet resultSet = null;
		int clickCount = 0;
		CapEntity capEntity;
		ClickEvent clickEvent;
		
		try {
			sqlServerDBInfo = clientDaoCapKeyInfo.getPrimarySqlServer();	  
			mssqlConn = sqlServerDBInfo.getConnection();
		} catch (Throwable exc) {
			try {
				logger.error("Primary SQL Server not available at " + sqlServerDBInfo.getDbJdbcUrl(), exc);
				sqlServerDBInfo = clientDaoCapKeyInfo.getStandbySqlServer();	  
				mssqlConn = sqlServerDBInfo.getConnection();							
			} catch (Throwable exc2) {
				logger.error("Standby SQL Server not available at " + sqlServerDBInfo.getDbJdbcUrl(), exc);
				throw exc2;
			}
		}
		
		try {		
			clickEvent = capEntityDetails.getClickEvent();
			capEntity = capEntityDetails.getCapEntity();
			mssqlGetInitStoredProc = mssqlConn.prepareCall ("{call [get_usage_totals_for_caps]  (?, ?, ?, ?, ?, ?, ?)}");
			mssqlGetInitStoredProc.setInt (1, capEntityDetails.getCountType().getDbId());
			mssqlGetInitStoredProc.setInt(2, capEntity.getCapEntityType().getDbId());
			mssqlGetInitStoredProc.setInt(3, capEntity.getId()); 
			if (clickEvent.getStartDate() != null) {
				mssqlGetInitStoredProc.setDate(4, new java.sql.Date (clickEvent.getStartDate().getTime()));				
			} else {
				mssqlGetInitStoredProc.setNull(4, java.sql.Types.DATE);								
			}
			if (clickEvent.getEndDate() != null) {
				mssqlGetInitStoredProc.setDate(5, new java.sql.Date (clickEvent.getEndDate().getTime()));				
			} else {
				mssqlGetInitStoredProc.setNull(5, java.sql.Types.DATE);												
			}
			
			if (capEntity.getMicroEvent() != null) {
				mssqlGetInitStoredProc.setInt(6, capEntity.getMicroEvent().getEventId());				
			} else {
				mssqlGetInitStoredProc.setNull(6, java.sql.Types.INTEGER);				
			}
			mssqlGetInitStoredProc.registerOutParameter(7, java.sql.Types.INTEGER);
			
			mssqlGetInitStoredProc.execute();
			clickCount = mssqlGetInitStoredProc.getInt(7);
			/*
			boolean result;
			resultSet = mssqlGetInitStoredProc.executeQuery(); 
			if (resultSet.next()) {
				clickCount = resultSet.getInt("clicks");
			} */
		}
		catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
		}
		finally {
		  this.closeDBResources(null, mssqlGetInitStoredProc, resultSet);
		}		  
		return clickCount;
	}		

	public int getOfferContractClickCount (ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityCountInfo) throws Throwable {
		MsSqlCapCountDao sqlServerDBInfo;
		Connection mssqlConn = null;
		CallableStatement mssqlGetInitStoredProc = null;
		java.sql.ResultSet resultSet = null;
		int clicks = 0;

		try {
			sqlServerDBInfo = clientDaoCapKeyInfo.getPrimarySqlServer();	  
			mssqlConn = sqlServerDBInfo.getConnection();
			mssqlGetInitStoredProc = mssqlConn.prepareCall ("{call [report_master_offer_contract]  (?, ?, ?, ?)}");
			mssqlGetInitStoredProc.setDate(1, new java.sql.Date (capEntityCountInfo.getClickEvent().getStartDate().getTime()));
			mssqlGetInitStoredProc.setDate(2, new java.sql.Date (capEntityCountInfo.getClickEvent().getEndDate().getTime()));
			mssqlGetInitStoredProc.setNull(3, java.sql.Types.INTEGER);
			mssqlGetInitStoredProc.setInt(4, capEntityCountInfo.getCapEntity().getId());
			resultSet = mssqlGetInitStoredProc.executeQuery(); 
			if (resultSet.next()) {
				clicks = resultSet.getInt("clicks");
			}

		}
		catch (Throwable exc) {
		     logger.error("", exc);
		     throw exc;
		}
		finally {
		  this.closeDBResources(null, mssqlGetInitStoredProc, resultSet);
		}		  
		return clicks;
	}		
	
	public int getOfferClickCount (ClientDaoCapKeyInfo clientDaoCapKeyInfo, CapEntityDetails capEntityCountInfo) throws Throwable {
		MsSqlCapCountDao sqlServerDBInfo;
		Connection mssqlConn = null;
		CallableStatement mssqlGetInitStoredProc = null;
		java.sql.ResultSet resultSet = null;
		int clicks = 0;

		try {
			sqlServerDBInfo = clientDaoCapKeyInfo.getPrimarySqlServer();	  
			mssqlConn = sqlServerDBInfo.getConnection();
			mssqlGetInitStoredProc = mssqlConn.prepareCall ("{call [report_master_offer]  (?, ?, ?, ?, ?, ?)}");
			mssqlGetInitStoredProc.setDate(1, new java.sql.Date (capEntityCountInfo.getClickEvent().getStartDate().getTime()));
			mssqlGetInitStoredProc.setDate(2, new java.sql.Date (capEntityCountInfo.getClickEvent().getEndDate().getTime()));
			mssqlGetInitStoredProc.setNull(3, java.sql.Types.INTEGER);
			mssqlGetInitStoredProc.setNull(4, java.sql.Types.INTEGER);
			mssqlGetInitStoredProc.setNull(5, java.sql.Types.INTEGER);
			mssqlGetInitStoredProc.setInt(6, capEntityCountInfo.getCapEntity().getId());
			resultSet = mssqlGetInitStoredProc.executeQuery(); 
			if (resultSet.next()) {
				clicks = resultSet.getInt("clicks");
			}

		}
		catch (Throwable exc) {
			logger.error("", exc);
			throw exc;
		}
		finally {
		  this.closeDBResources(null, mssqlGetInitStoredProc, resultSet);
		}		  
		return clicks;
	}		
	
	
}
