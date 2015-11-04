package com.getcake.geo.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.log4j.*;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
/*
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
*/

abstract public class BaseDao { // extends JdbcDaoSupport {
	
	protected static final Logger logger = Logger.getLogger(BaseDao.class);
	
	// protected NamedParameterJdbcTemplate  namedParameterJdbcTemplate ;
	protected String dbJdbcUrl, dbDriver;
	protected Properties properties;
	
	protected Connection getConnection() throws SQLException {
		try {
			Class.forName(dbDriver);			
		} catch (ClassNotFoundException exc) {
			throw new RuntimeException (exc);
		}
		return DriverManager.getConnection(dbJdbcUrl, properties); // dataSource.getConnection();
	}
	
	protected DataSource dataSource;
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		// namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	public DataSource getDataSource () {
		return this.dataSource;
	}
	
	public BaseDao () {
		/*
		JndiTemplate jndi = new JndiTemplate();
		try {
            this.setDataSource((DataSource) jndi.lookup("java:comp/env/jdbc/txmDatasource"));
        } catch (NamingException exc) {
        	logger.error(exc);
        	throw new RuntimeException (exc);
            // logger.error("NamingException for java:comp/env/jdbc/yourname", e);
        }		
		*/
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    byte[] data = new byte[len / 2];
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}
	
	public void closeDBResources(Connection sourceConn, PreparedStatement sourceStmt, ResultSet sourceRs) {
		try {
			if (sourceRs != null) {
				sourceRs.close();
			}
		} catch (Throwable exc) {
			logger.error("", exc);
		}

		try {
			if (sourceStmt != null) {
				sourceStmt.close();
			}
		} catch (Throwable exc) {
			logger.error("", exc);
		}

		try {
			if (sourceConn != null) {
				sourceConn.close();
			}
		} catch (Throwable exc) {
			logger.error("", exc);
		}
	}

	
	
}
