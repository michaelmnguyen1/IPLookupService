package com.getcake.capcount.model;

import com.datastax.driver.core.Session;

public class KeySpaceInfo {
    private String countKeySpace;
    private Session countKeySpaceSession;

    public void setCountKeySpace (String countKeySpace) {
    	this.countKeySpace = countKeySpace;
    }

    public String getCountKeySpace () {
    	return this.countKeySpace;
    }
    
    public void setCountKeySpaceSession (Session countKeySpaceSession) {
    	this.countKeySpaceSession = countKeySpaceSession;
    }

    public Session getCountKeySpaceSession () {
    	return this.countKeySpaceSession;
    }
    
    
}
