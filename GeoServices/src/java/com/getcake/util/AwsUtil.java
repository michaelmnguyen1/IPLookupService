package com.getcake.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.amazonaws.util.EC2MetadataUtils;

public class AwsUtil {
	private static final Logger logger = Logger.getLogger(AwsUtil.class);

	public static final String DEFAULT_AWS_REGION = "us-west-2";
	
	private static final Properties loadPropertiesByRegion (String propFileName) throws IOException {
		Properties properties;
		String fullPropFileName, availZone, zoneFullPropFileName, msg;
		FileInputStream propFileInputStream;
		File tmpFile;
		
        String absCurrPath = Paths.get("").toAbsolutePath().toString();
		/*
        logger.debug("Current toAbsolutePath path is: " + absCurrPath);
        
	    fullPropFileName = absCurrPath + "/" + propFileName;
		logger.error("Loading properties file: " + fullPropFileName);
	    propFileInputStream = new FileInputStream (fullPropFileName);
	    properties = new Properties ();
		properties.load(propFileInputStream);			
    	if (true) return properties;
		*/
    	
        availZone = EC2MetadataUtils.getAvailabilityZone ();
	    logger.debug("AvailabilityZone: " + availZone);
	    if (availZone == null) {
		    fullPropFileName = absCurrPath + "/" + propFileName;
	    	tmpFile = new File(fullPropFileName); 
	    	if (! tmpFile.exists()) {
	    		msg = "No properties file exist at global level: " + fullPropFileName;
	    		logger.error(msg);
	    		throw new RuntimeException(msg);
	    	}
	    } else {
		    fullPropFileName = absCurrPath + "/" + propFileName + "." + availZone;
		    zoneFullPropFileName = fullPropFileName;
	    	tmpFile = new File(fullPropFileName); 
	    	if (! tmpFile.exists()) {    		
		    	availZone = availZone.substring(0, availZone.lastIndexOf('-'));
			    fullPropFileName = absCurrPath + "/" + propFileName + "." + availZone;
		    	tmpFile = new File(fullPropFileName); 
		    	if (! tmpFile.exists()) {
				    fullPropFileName = absCurrPath + "/" + propFileName;
			    	tmpFile = new File(fullPropFileName); 
			    	if (! tmpFile.exists()) {
			    		msg = "No properties file exist at zone, region, or global levels: " + zoneFullPropFileName +
				    			", " + fullPropFileName + ", or " + fullPropFileName;
			    		logger.error(msg);
			    		throw new RuntimeException(msg);
			    	}
		    	}
	    	} 	    	
	    }
	    
		logger.debug("Loading properties file: " + fullPropFileName);
	    propFileInputStream = new FileInputStream (fullPropFileName);
	    properties = new Properties ();
		properties.load(propFileInputStream);			
    	return properties;
	}
	
	public static final Properties loadProperties (String propFileName) throws IOException {
		// com.fasterxml.jackson.databind.ser.std.SqlDateSerializer a;
		Properties properties;
		String fullPropFileName, availZone, msg;
		FileInputStream propFileInputStream;
		File tmpFile;
		
        String absCurrPath = Paths.get("").toAbsolutePath().toString();
	    fullPropFileName = absCurrPath + "/" + propFileName;
    	tmpFile = new File(fullPropFileName); 
    	if (! tmpFile.exists()) {    		
    		msg = "No properties file exist at " + fullPropFileName;
    		logger.error(msg);
    		throw new RuntimeException(msg);
    	} 	    	
		logger.debug("Loading properties file: " + fullPropFileName);
	    propFileInputStream = new FileInputStream (fullPropFileName);
	    properties = new Properties ();
		properties.load(propFileInputStream);
		
        availZone = EC2MetadataUtils.getAvailabilityZone ();
	    if (availZone != null) {
			properties.put("availabilityZone", availZone);
			properties.put("region", availZone.substring(0,  availZone.length() - 1));	    	
	    } else {
			properties.put("region", DEFAULT_AWS_REGION);	    	
	    }
	    logger.debug("region: " + properties.getProperty("region"));		
    	return properties;
	}
	
	public static String getAwsRegion () {
		String availZone;
		
        availZone = EC2MetadataUtils.getAvailabilityZone ();
	    if (availZone == null) {
	    	return "";
	    }
        return availZone.substring(0, availZone.lastIndexOf('-'));
	}
}
