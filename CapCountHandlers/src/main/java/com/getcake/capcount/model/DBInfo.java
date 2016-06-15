package com.getcake.capcount.model;

import java.sql.Connection;
import java.util.Properties;

public class DBInfo {
	private String url, userName, password;

	public DBInfo () {
	}
	
	public void setUrl (String url) {
		this.url = url;
	}
	
	public String setUrl () {
		return this.url;
	}

	public void setUserName (String userName) {
		this.userName = userName;
	}
	
	public String getUserName () {
		return this.userName;
	}
	
	public void setPassword (String password) {
		this.password = password;
	}
	
	public String getPassword () {
		return this.password;
	}
	
}
