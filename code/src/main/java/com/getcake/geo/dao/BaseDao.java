package com.getcake.geo.dao;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.support.JdbcDaoSupport;

import com.getcake.geo.controller.GeoController;

abstract public class BaseDao { // extends JdbcDaoSupport {
	
	protected static final Logger logger = LoggerFactory.getLogger(BaseDao.class);
	
	protected DataSource dataSource;
	
	protected NamedParameterJdbcTemplate  namedParameterJdbcTemplate ;
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
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
	
	
	
}
