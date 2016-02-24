/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.getcake.sparkjdbc;

import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.delete;
import spark.Spark;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.text.SimpleDateFormat;

import javax.servlet.http.HttpServletResponse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.thriftserver.*; 
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getcake.geo.controller.GeoController;
import com.getcake.geo.model.MsSqlExportCheckResp;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class SparkJDBCServer {

	private static final String CODE_VERSION = "0.9";
	private static final Logger logger = Logger.getLogger(SparkJDBCServer.class);
    // private static int ipv6CityCount, citiesCount, ipv6IspCount, ispsCount;

    private String ipv6_cityFileName, citiesFileName, ipv6_ispFileName, ispsFileName, sparkMaster, appName, sparkConfigFileName;
    
    private String geoSourceFilePath, geoDataVersion; // "s3://cake-deployment-artifacts-us-west/ipdata/"; //  
    private JavaSparkContext javaSparkContext;
    private HiveContext hiveContext;
    private SparkConf sparkConf;
    private Properties properties;
    private AWSCredentialsProvider credentialsProvider;
    private Configuration hadoopConf;
    private boolean cacheFlag, preloadTableFlag, inferSchemaFlag, headerInCSVFileFlag;
    private String topLevelDataPath, region;
    private GeoController geoController; 

    public static void main(String[] args) throws Exception {
    	SparkJDBCServer sparkJDBCServer;
    	
    	sparkJDBCServer = new SparkJDBCServer ();
    	sparkJDBCServer.start(args);
    }
    
    public void start (String[] args) throws Exception {	    	
    	int sparkWebMinThreads, sparkWebMaxThreads, sparkWebIdleTimeoutMillis, sparkWebPort;
    	
	    try {		  
		    if (args == null ||  args.length != 1) {
		    	log ("=== SparkJDBCServer " + CODE_VERSION + " - usage: <spark config file>");
		    	System.exit (-1);
		    }
	    	sparkConfigFileName = args[0];
	    	log ("=== SparkJDBCServer " + CODE_VERSION + " - sparkConfigFileName: " + sparkConfigFileName);

            geoController = GeoController.getInstance();
	    	// properties = AwsUtil.loadProperties(sparkConfigFileName);    	
            properties = geoController.initWithMsSqlDao(sparkConfigFileName);	    	
		    sparkMaster = properties.getProperty("spark.master"); 
		    appName = properties.getProperty("spark.app.name"); 

		    sparkConf = new SparkConf();       	
		    if ("local".equalsIgnoreCase(sparkMaster)) {
			    sparkConf.setMaster(sparkMaster); 		    
			    sparkConf.setAppName(appName);
			    sparkConf.set("spark.executor.memory", properties.getProperty("spark.executor.memory"));
			    sparkConf.set("spark.driver.memory", properties.getProperty("spark.driver.memory"));
		    }
		    
		    log ("sparkMaster: " + sparkMaster);
		    log ("spark.executor.memory: " + sparkConf.get("spark.executor.memory"));
		    log ("spark.driver.memory: " + sparkConf.get("spark.driver.memory"));		    	

		    javaSparkContext = new JavaSparkContext(sparkConf);
		    
		    if ("ProfileCredentialsProvider".equalsIgnoreCase(properties.getProperty("credentialsProvider"))) {
			    log ("credentialsProvider: ProfileCredentialsProvider");
			    credentialsProvider =  new ProfileCredentialsProvider();	    			    		    	
		    } else {
			    log ("credentialsProvider: InstanceProfileCredentialsProvider");
			    credentialsProvider =  new InstanceProfileCredentialsProvider();	    			    	
		    }
		    hadoopConf = javaSparkContext.sc().hadoopConfiguration();
		    
		    hadoopConf.set("fs.s3.awsAccessKeyId", credentialsProvider.getCredentials().getAWSAccessKeyId()); 
    	    hadoopConf.set("fs.s3.awsSecretAccessKey", credentialsProvider.getCredentials().getAWSSecretKey());
		    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
		    
		    hadoopConf.set("fs.s3n.awsAccessKeyId", credentialsProvider.getCredentials().getAWSAccessKeyId()); 
    	    hadoopConf.set("fs.s3n.awsSecretAccessKey", credentialsProvider.getCredentials().getAWSSecretKey());
    	    hadoopConf.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem");
    	    
		    hadoopConf.set("fs.s3a.awsxxAccessKeyId", credentialsProvider.getCredentials().getAWSAccessKeyId()); 
    	    hadoopConf.set("fs.s3a.awsSecretAccessKey", credentialsProvider.getCredentials().getAWSSecretKey());
    	    hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem");
    	    hadoopConf.set("fs.s3a.connection.ssl.enabled","false");
    	    hadoopConf.set("fs.s3a.connection.maximum","false");
		    
		    hiveContext = new HiveContext (javaSparkContext.sc());
		    
		    // hiveContext.sparkContext().addSparkListener(listener);
		    // javaSparkContext.add
		    
		    // DataFrame citiesDF = hiveContext.read().load("C:/Projects/GeoServices/geodata/cities.csv");
		    // citiesDF.registerTempTable("cities");

		    // hiveContext.sql("CREATE TABLE IF NOT EXISTS cities (locationId INT, country STRING, region String, city String, latitude float, longitude float, metroCode String )");
		    // hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Projects/GeoServices/geodata/cities.csv' INTO TABLE cities");
		    // hiveContext.sql("CREATE TABLE IF NOT EXISTS cities (country STRING)");
		    // hiveContext.sql("LOAD DATA LOCAL INPATH 'C:/Projects/GeoServices/geodata/cities-sample.csv' INTO TABLE cities");
		    // log ("HiveThriftServer2.startWithContext loaded table cities");
		    // HiveThriftServer2.listener_$eq(arg0);
    	    
		    topLevelDataPath = properties.getProperty("topLevelDataPath");
		    region = properties.getProperty("region"); 
		    if (region != null) {
		    	topLevelDataPath += "-" + region;
		    }
		    		    		    
		    initGeoData (topLevelDataPath);
		    
		    HiveThriftServer2.startWithContext(hiveContext);
		    log ("=== Spark JDBC Server started");
		    
	    	sparkWebMinThreads = Integer.parseInt(properties.getProperty("sparkWebMinThreads")); 
	    	sparkWebMaxThreads = Integer.parseInt(properties.getProperty("sparkWebMaxThreads")); 
	    	sparkWebIdleTimeoutMillis = Integer.parseInt(properties.getProperty("sparkWebIdleTimeoutMillis"));
	    	sparkWebPort = Integer.parseInt(properties.getProperty("sparkWebPort")); 
        	Spark.port(sparkWebPort);
        	Spark.threadPool(sparkWebMaxThreads, sparkWebMinThreads, sparkWebIdleTimeoutMillis);
		    
            get("/geoservices/status", (request, response) -> {
                return "Spark JDBC Server Working";
            });
            
            post ("/sparksql/geosourcefilepath", (request, response) -> {
            	geoSourceFilePath = request.queryParams("geosourcefilepath");      
            	return "geoSourceFilePath set to " + geoSourceFilePath;
            }); 
            
            get ("/sparksql/geosourcefilepath", (request, response) -> {
            	return geoSourceFilePath;
            }); 

            post ("/sparksql/geodataversion", (request, response) -> {
                geoDataVersion = request.queryParams("geodataversion");
                return geoDataVersion;
            }); 
                        
            get ("/sparksql/geodataversion", (request, response) -> {
                return geoDataVersion;
            }); 
                                    
            post ("/sparksql/geodata", (request, response) -> {
            	try {
                    geoDataVersion = request.queryParams("geodataversion");
                    return loadGeoDataByVersion (geoDataVersion);            		
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		exc.printStackTrace();
            		log ("/sparksql/geodata", exc);
            		return exc.getLocalizedMessage();
            	}
            }); 
                        
            delete ("/sparksql/geodata", (request, response) -> {
            	try {
                    geoDataVersion = request.queryParams("geodataversion");
                    return unloadGeoDataByVersion (geoDataVersion);            		
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		exc.printStackTrace();
            		log ("/sparksql/geodata", exc);
            		return exc.getLocalizedMessage();
            	}
            }); 
                        
            get ("/sparksql/geodata", (request, response) -> {
                return geoDataVersion;
            }); 
                        
            post("/sparksql/geotable", (request, response) -> {
            	String tableName = null, fullPathTableName = null, respMsg;
            	
            	try {            		
                	tableName = request.queryParams("tablename");                        
                	fullPathTableName = geoSourceFilePath + geoDataVersion + "/" + tableName + geoDataVersion + ".csv";
                	return loadTable(tableName + geoDataVersion, fullPathTableName);
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		respMsg = "error loading table: " + fullPathTableName + " - err:" + exc.getLocalizedMessage();
            		exc.printStackTrace();
            		log ("/sparksql/loadtable", exc);
            		return respMsg;
            	}            	
            });
        		
            // new 
            post ("/sparksql/table", (request, response) -> {
            	String tableName = null, fullPathTableName = null, respMsg, fileName, metaFileName, fileListName;
            	Boolean tmpheaderInCSVFileFlag = headerInCSVFileFlag;
            	
            	try {            		
                	tableName = request.queryParams("tablename");                        
                	metaFileName = request.queryParams("metafilename");
                	fileListName = request.queryParams("filelistname");                                        	
                	
                	fileName = request.queryParams("filename");                        
                	/* headerInCSVFileStr = request.queryParams("headerincsvfile");
                	if (headerInCSVFileStr != null) {
                		headerInCSVFileFlag = Boolean.parseBoolean(headerInCSVFileStr);
                	} */
                	fullPathTableName = geoSourceFilePath +"/" + fileName;
                	metaFileName = geoSourceFilePath +"/" + metaFileName;
                	return loadFilesWithMeta(tableName, fullPathTableName, metaFileName, fileListName);
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		respMsg = "error loading table: " + fullPathTableName + " - err:" + exc.getLocalizedMessage();
            		exc.printStackTrace();
            		log ("/sparksql/loadtable", exc);
            		return respMsg;
            	} finally {
            		// headerInCSVFileFlag = tmpheaderInCSVFileFlag;
            	}
            });
        				
            delete ("/sparksql/table", (request, response) -> {
            	String tableName = null, fullPathTableName = "N/A", respMsg, fileName;
            	
            	try {            		
                	tableName = request.queryParams("tablename");                        
                	return unloadTable(tableName, fullPathTableName);
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		respMsg = "error loading table: " + fullPathTableName + " - err:" + exc.getLocalizedMessage();
            		exc.printStackTrace();
            		log ("/sparksql/loadtable", exc);
            		return respMsg;
            	}            	
            });
        				
            post ("/sparksql/mssqldata", (request, response) -> {
            	StringBuilder respMsg;
            	
            	try {
            		respMsg = new StringBuilder ();
                    geoDataVersion = request.queryParams("geodataversion");
                    respMsg.append(geoController.exportMsSqlGeoData(geoDataVersion));
            		respMsg.append(System.getProperty("line.separator"));								
                    return respMsg.append(loadGeoDataByVersion (geoDataVersion));            		
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		exc.printStackTrace();
            		log ("/sparksql/geodata", exc);
            		return exc.getLocalizedMessage();
            	}
            }); 
                        
            post ("/sparksql/mssqlversioncheck", (request, response) -> {
            	StringBuilder respMsg;
            	MsSqlExportCheckResp msSqlExportCheckResp;
            	ObjectMapper jsonMapper;

            	try {
                    jsonMapper = new ObjectMapper ();
            		respMsg = new StringBuilder ();
            		msSqlExportCheckResp = geoController.exportMsSqlGeoData();
            		if (msSqlExportCheckResp.newIpVersion == null || msSqlExportCheckResp.newIpVersion.trim().length() == 0) {
                        return jsonMapper.writeValueAsString(msSqlExportCheckResp);
            		}
            		respMsg.append(msSqlExportCheckResp.detailMsg);
            		respMsg.append(System.getProperty("line.separator"));		
                    respMsg.append(loadGeoDataByVersion (msSqlExportCheckResp.newIpVersion));
                    msSqlExportCheckResp.detailMsg = respMsg.toString();
                    return jsonMapper.writeValueAsString(msSqlExportCheckResp);
            	} catch (Throwable exc) {
            		response.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            		log ("", exc);
            		return exc.getLocalizedMessage();
            	}
            }); 
                        
		    log ("=== Spark JDBC Web Services started");
	  } catch (Throwable exc) {
		  log ("main", exc);
	  }
    }

    private String unloadGeoDataByVersion (String geoDataVersion) {
        String ipv6_cityPathFileName, citiesPathFileName, ipv6_ispPathFileName, ispsPathFileName;
        StringBuilder respMsg = new StringBuilder ();

    	citiesPathFileName = geoSourceFilePath + geoDataVersion + "/" + citiesFileName + geoDataVersion + ".csv";
    	respMsg.append (unloadTable(citiesFileName + geoDataVersion, citiesPathFileName));
    	respMsg.append (",");
    	
    	ipv6_cityPathFileName = geoSourceFilePath + geoDataVersion + "/" + ipv6_cityFileName + geoDataVersion + ".csv";
    	respMsg.append (unloadTable(ipv6_cityFileName + geoDataVersion, ipv6_cityPathFileName));
    	respMsg.append (",");
    	
	    ipv6_ispPathFileName = geoSourceFilePath + geoDataVersion + "/" + ipv6_ispFileName + geoDataVersion + ".csv";
	    respMsg.append (unloadTable(ipv6_ispFileName + geoDataVersion, ipv6_ispPathFileName));
    	respMsg.append (",");
    	
    	ispsPathFileName = geoSourceFilePath + geoDataVersion + "/" + ispsFileName + geoDataVersion + ".csv";
    	respMsg.append (unloadTable(ispsFileName + geoDataVersion, ispsPathFileName));
    	
    	return respMsg.toString();
    }
    
    private String unloadTable (String registerTableName, String fullPathTableName) {
    	String respMsg;

	    try {
	    	hiveContext.uncacheTable(registerTableName);
	    	logger.debug("dropping table " + registerTableName);
	    	hiveContext.dropTempTable(registerTableName);
	    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was dropped";
	    	log (respMsg);
	    	return respMsg;
	    } catch (Throwable exc) { 
	    	// hiveContext.table does not declare that it throws NoSuchTableException, so cannot use it in catch clause and
	    	// have to check for it explicitly
	    	if (exc instanceof NoSuchTableException) {
		    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was not loaded => do nothing for unload";
		    	log (respMsg);	    		
		    	return respMsg;
	    	} else {
	    		throw exc;
	    	}
	    }	    
    }
    
    private String loadGeoDataByVersion (String newGeoDataVersion) {
        String ipv6_cityPathFileName, citiesPathFileName, ipv6_ispPathFileName, ispsPathFileName;
        StringBuilder respMsg = new StringBuilder ();

    	citiesPathFileName = geoSourceFilePath + newGeoDataVersion + "/" + citiesFileName + newGeoDataVersion + ".csv";
    	respMsg.append (loadTable(citiesFileName + newGeoDataVersion, citiesPathFileName));
    	respMsg.append (",");
    	
    	ipv6_cityPathFileName = geoSourceFilePath + newGeoDataVersion + "/" + ipv6_cityFileName + newGeoDataVersion + ".csv";
    	respMsg.append (loadTable(ipv6_cityFileName + newGeoDataVersion, ipv6_cityPathFileName));
    	respMsg.append (",");
    	
	    ipv6_ispPathFileName = geoSourceFilePath + newGeoDataVersion + "/" + ipv6_ispFileName + newGeoDataVersion + ".csv";
	    respMsg.append (loadTable(ipv6_ispFileName + newGeoDataVersion, ipv6_ispPathFileName));
    	respMsg.append (",");
    	
    	ispsPathFileName = geoSourceFilePath + newGeoDataVersion + "/" + ispsFileName + newGeoDataVersion + ".csv";
    	respMsg.append (loadTable(ispsFileName + newGeoDataVersion, ispsPathFileName));
    	
    	geoDataVersion = newGeoDataVersion;
    	return respMsg.toString();
    }
    
    private String loadSingleFileWithMeta (String registerTableName, String fullPathTableName, String metaFileName) throws IOException {
    	DataFrame dynamicDataFrame;
    	long startTime, firstStartTime;
    	float durSeconds, durMinutes;
    	String respMsg;

    	startTime = System.currentTimeMillis();
	    firstStartTime = startTime;
	    try {
		    dynamicDataFrame = hiveContext.table(registerTableName);	    	
	    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was already loaded";
	    	log (respMsg);
	    	return respMsg;
	    } catch (Throwable exc) { 
	    	// hiveContext.table does not declare that it throws NoSuchTableException, so cannot use it in catch clause and
	    	// have to check for it explicitly
	    	if (exc instanceof NoSuchTableException) {
		    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was not loaded => load it next";
		    	log (respMsg);	    		
	    	} else {
	    		throw exc;
	    	}
	    }
	    
		FileInputStream propFileInputStream;
	    propFileInputStream = new FileInputStream (metaFileName);
	    properties = new Properties ();
		properties.load(propFileInputStream);
		
		Stream<Entry<Object, Object>> stream = properties.entrySet().stream();
		Map<String, String> options = stream.collect(Collectors.toMap(
	            entry -> String.valueOf(entry.getKey()),
	            entry -> String.valueOf(entry.getValue())));

		int numColumns = Integer.parseInt(properties.getProperty("numColumns"));
    	StructField structFields [] = new StructField [numColumns];
		String colName, colType;
    	StructField structField;
		
		for (int i = 1; i <= numColumns; i++) {
			colName = properties.getProperty("col" + i + ".name");
			colType = properties.getProperty("col" + i + ".type");
			switch (colType) {
			case "TimeStamp":
				structField = DataTypes.createStructField(colName, DataTypes.TimestampType, true);
				break;
			
			case "Date":
				structField = DataTypes.createStructField(colName, DataTypes.DateType, true);
				break;
			
			case "Float":
				structField = DataTypes.createStructField(colName, DataTypes.FloatType, true);
				break;
			
			case "Integer":
				structField = DataTypes.createStructField(colName, DataTypes.IntegerType, true);
				break;
			
			case "Long":
				structField = DataTypes.createStructField(colName, DataTypes.LongType, true);
				break;
			
			case "Short":
				structField = DataTypes.createStructField(colName, DataTypes.ShortType, true);
				break;
			
			case "Double":
				structField = DataTypes.createStructField(colName, DataTypes.DoubleType, true);
				break;
			
			case "Boolean":
				structField = DataTypes.createStructField(colName, DataTypes.BooleanType, true);
				break;
			
			case "Binary":
				structField = DataTypes.createStructField(colName, DataTypes.BinaryType, true);
				break;
			
			case "Byte":
				structField = DataTypes.createStructField(colName, DataTypes.ByteType, true);
				break;
			
			case "Null":
				structField = DataTypes.createStructField(colName, DataTypes.NullType, true);
				break;
			
			default:
				structField = DataTypes.createStructField(colName, DataTypes.StringType, true);
			}
			
			structFields[i - 1] = structField;
		}
		
		
    	 // dynamicDataFrame = hiveContext.read().format("com.databricks.spark.csv").
    	//	option("header", Boolean.toString(headerInCSVFileFlag)).option("inferSchema", Boolean.toString(inferSchemaFlag)).load(fullPathTableName);
    	// Map<String, String> options = new HashMap<>(properties);
    	options.put("path", "file:///" + fullPathTableName);
    	// options.put("header", "false");
    	// options.put("delimiter", ",");
    	
    	// DataType dataType = new DataType ();
    	/*
    	StructField structField1 = DataTypes.createStructField("LogType", DataTypes.StringType, false);
    	StructField structField2 = DataTypes.createStructField("EntryTime", DataTypes.TimestampType, false);
    	StructField structField3 = DataTypes.createStructField("Code_Class", DataTypes.StringType, false);
    	StructField structField4 = DataTypes.createStructField("Code_Method", DataTypes.StringType, false);
    	StructField structField5 = DataTypes.createStructField("Log_Message", DataTypes.StringType, false);
    	structFields[0] = structField1;
    	structFields[1] = structField2;
    	structFields[2] = structField3;
    	structFields[3] = structField4;
    	structFields[4] = structField5;
    	*/
    	
    	StructType schema = new StructType(structFields);

    	dynamicDataFrame = hiveContext.load("com.databricks.spark.csv", schema, options);
    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("loaded table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
    	
    	schema = dynamicDataFrame.schema();
    	structFields = schema.fields();
    	for (StructField structFieldLocal : structFields) {
	    	DataType dataType = structFieldLocal.dataType();
    		logger.debug(structFieldLocal.name() + " - dataType: " + dataType.typeName());
    	}
    	
	    startTime = System.currentTimeMillis();
    	dynamicDataFrame.cache();		    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("cache table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

	    startTime = System.currentTimeMillis();
	    dynamicDataFrame.registerTempTable(registerTableName);
	    
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("registerTempTable table " + registerTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
	    
    	durSeconds = (float)(System.currentTimeMillis() - firstStartTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	respMsg = "Completed loading table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes;
    	log (respMsg);
        return respMsg;    	
    }
    
    private String loadFilesWithMeta (String registerTableName, String fullPathTableName, String metaFileName, String fileListName) throws IOException {
    	DataFrame combinedDynamicDataFrame = null, dynamicDataFrame = null;
    	long startTime, firstStartTime;
    	float durSeconds, durMinutes;
    	String respMsg;

    	startTime = System.currentTimeMillis();
	    firstStartTime = startTime;
	    try {
		    combinedDynamicDataFrame = hiveContext.table(registerTableName);	    	
	    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was already loaded";
	    	log (respMsg);
	    	return respMsg;
	    } catch (Throwable exc) { 
	    	// hiveContext.table does not declare that it throws NoSuchTableException, so cannot use it in catch clause and
	    	// have to check for it explicitly
	    	if (exc instanceof NoSuchTableException) {
		    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was not loaded => load it next";
		    	log (respMsg);	    		
	    	} else {
	    		throw exc;
	    	}
	    }
	    
	    
		FileInputStream propFileInputStream;
	    propFileInputStream = new FileInputStream (metaFileName);
	    properties = new Properties ();
		properties.load(propFileInputStream);
		
		Stream<Entry<Object, Object>> stream = properties.entrySet().stream();
		Map<String, String> options = stream.collect(Collectors.toMap(
	            entry -> String.valueOf(entry.getKey()),
	            entry -> String.valueOf(entry.getValue())));

		int numColumns = Integer.parseInt(properties.getProperty("numColumns"));
    	StructField structFields [] = new StructField [numColumns];
		String colName, colType;
    	StructField structField;
		
		// structField = DataTypes.createStructField("File_Source", DataTypes.StringType, true);
		// structFields[0] = structField;
		
		for (int i = 1; i <= numColumns; i++) {
			colName = properties.getProperty("col" + i + ".name");
			colType = properties.getProperty("col" + i + ".type");
			switch (colType) {
			case "TimeStamp":
				structField = DataTypes.createStructField(colName, DataTypes.TimestampType, true);
				break;
			
			case "Date":
				structField = DataTypes.createStructField(colName, DataTypes.DateType, true);
				break;
			
			case "Float":
				structField = DataTypes.createStructField(colName, DataTypes.FloatType, true);
				break;
			
			case "Integer":
				structField = DataTypes.createStructField(colName, DataTypes.IntegerType, true);
				break;
			
			case "Long":
				structField = DataTypes.createStructField(colName, DataTypes.LongType, true);
				break;
			
			case "Short":
				structField = DataTypes.createStructField(colName, DataTypes.ShortType, true);
				break;
			
			case "Double":
				structField = DataTypes.createStructField(colName, DataTypes.DoubleType, true);
				break;
			
			case "Boolean":
				structField = DataTypes.createStructField(colName, DataTypes.BooleanType, true);
				break;
			
			case "Binary":
				structField = DataTypes.createStructField(colName, DataTypes.BinaryType, true);
				break;
			
			case "Byte":
				structField = DataTypes.createStructField(colName, DataTypes.ByteType, true);
				break;
			
			case "Null":
				structField = DataTypes.createStructField(colName, DataTypes.NullType, true);
				break;
			
			default:
				structField = DataTypes.createStructField(colName, DataTypes.StringType, true);
			}
			
			structFields[i - 1] = structField;
		}
		
    	StructType schema = new StructType(structFields);

    	List<String> fileLlist = new ArrayList<>();
    	try (BufferedReader br = Files.newBufferedReader(Paths.get(fileListName))) {

			//br returns as stream and convert it into a List
			fileLlist = br.lines().collect(Collectors.toList());

		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	for (String file : fileLlist) {
        	options.put("path", "file:///" + file);
	    	dynamicDataFrame = hiveContext.load("com.databricks.spark.csv", schema, options);
	    	if (combinedDynamicDataFrame == null) {
	    		combinedDynamicDataFrame = dynamicDataFrame;
	    	} else {
	    		combinedDynamicDataFrame = combinedDynamicDataFrame.unionAll(dynamicDataFrame);	    		
	    	}
    	}
    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("loaded table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
    	
    	schema = combinedDynamicDataFrame.schema();
    	structFields = schema.fields();
    	for (StructField structFieldLocal : structFields) {
	    	DataType dataType = structFieldLocal.dataType();
    		logger.debug(structFieldLocal.name() + " - dataType: " + dataType.typeName());
    	}
    	
	    startTime = System.currentTimeMillis();
    	combinedDynamicDataFrame.cache();		    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("cache table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

	    startTime = System.currentTimeMillis();
	    combinedDynamicDataFrame.registerTempTable(registerTableName);
	    
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("registerTempTable table " + registerTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
	    
    	durSeconds = (float)(System.currentTimeMillis() - firstStartTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	respMsg = "Completed loading table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes;
    	log (respMsg);
        return respMsg;    	
    }
    
    private String loadTable (String registerTableName, String fullPathTableName) {
    	DataFrame dynamicDataFrame;
    	long startTime, firstStartTime;
    	float durSeconds, durMinutes;
    	String respMsg;

    	startTime = System.currentTimeMillis();
	    firstStartTime = startTime;
	    try {
		    dynamicDataFrame = hiveContext.table(registerTableName);	    	
	    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was already loaded";
	    	log (respMsg);
	    	return respMsg;
	    } catch (Throwable exc) { 
	    	// hiveContext.table does not declare that it throws NoSuchTableException, so cannot use it in catch clause and
	    	// have to check for it explicitly
	    	if (exc instanceof NoSuchTableException) {
		    	respMsg = "table " + registerTableName + " at " + fullPathTableName + " was not loaded => load it next";
		    	log (respMsg);	    		
	    	} else {
	    		throw exc;
	    	}
	    }
	    
    	 dynamicDataFrame = hiveContext.read().format("com.databricks.spark.csv").
    		option("header", Boolean.toString(headerInCSVFileFlag)).option("inferSchema", Boolean.toString(inferSchemaFlag)).load(fullPathTableName);
    	
    	StructType schema;
    	StructField structFields [];
    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("loaded table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
    	
    	schema = dynamicDataFrame.schema();
    	structFields = schema.fields();
    	for (StructField structField : structFields) {
	    	DataType dataType = structField.dataType();
    		logger.debug(structField.name() + " - dataType: " + dataType.typeName());
    	}
    	
	    startTime = System.currentTimeMillis();
    	dynamicDataFrame.cache();		    	
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("cache table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

	    startTime = System.currentTimeMillis();
	    dynamicDataFrame.registerTempTable(registerTableName);
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("registerTempTable table " + registerTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
	    
    	durSeconds = (float)(System.currentTimeMillis() - firstStartTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	respMsg = "Completed loading table " + fullPathTableName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes;
    	log (respMsg);
        return respMsg;    	
    }
    
    private void initGeoData (String topLevelDataPath) {
        
        geoDataVersion = properties.getProperty("geoDataVersion");
	    geoSourceFilePath = topLevelDataPath + "/" + properties.getProperty("geoTopLevelFolder") + "/";
	    ipv6_cityFileName = properties.getProperty("ipv6_cityFileName");
	    citiesFileName = properties.getProperty("citiesFileName");
	    ipv6_ispFileName = properties.getProperty("ipv6_ispFileName");
	    ispsFileName = properties.getProperty("ispsFileName");
	    	    
	    log ("geoDataVersion: " + geoDataVersion);
	    log ("geoSourceFilePath: " + geoSourceFilePath);
	    
	    if ("true".equalsIgnoreCase(properties.getProperty("inferSchema"))) {
	    	inferSchemaFlag = true;
	    } else {
	    	inferSchemaFlag = false;
	    }    	
	    log ("inferSchemaFlag: " + inferSchemaFlag);
	    
	    if ("true".equalsIgnoreCase(properties.getProperty("headerInCSVFile"))) {
	    	headerInCSVFileFlag = true;
	    } else {
	    	headerInCSVFileFlag = false;
	    }    	
	    log ("headerInCSVFileFlag: " + headerInCSVFileFlag);
	    
	    if ("true".equalsIgnoreCase(properties.getProperty("preloadTableFlag"))) {
	    	preloadTableFlag = true;
	    } else {
	    	preloadTableFlag = false;
	    }
	    log ("preloadTableFlag: " + preloadTableFlag);
	    
	    if ("true".equalsIgnoreCase(properties.getProperty("cacheFlag"))) {
	    	cacheFlag = true;
	    } else {
	    	cacheFlag = false;
	    }    	
	    log ("cacheFlag: " + cacheFlag);

	    /*
    	long startTime, firstStartTime;
    	float durSeconds, durMinutes;
	    if (true) return;
	    
    	citiesPathFileName = geoSourceFilePath + geoDataVersion + "/" + citiesFileName + geoDataVersion + ".csv";
    	log("geoInfoRDD source: " + citiesPathFileName);
	    startTime = System.currentTimeMillis();
    	firstStartTime = startTime;
    	    	
	    JavaRDD<GeoInfo> geoInfoRDD = javaSparkContext.textFile(citiesPathFileName).map(
	      new Function<String, GeoInfo>() {
	        @Override
	        public GeoInfo call(String line) {

	        	try {
		        	  String[] parts = line.split(",");
					  citiesCount++;
					  if ((citiesCount % 10_000) == 1) {
						  log ("citiesCount: " + citiesCount + " loading locationId: " + parts[0]);					  
					  }

					  GeoInfo geoInfo = new GeoInfo();
					  if (!"NULL".equalsIgnoreCase(parts[0]) || parts[0].trim().length() > 0) {
						  geoInfo.setLocationId(Integer.parseInt(parts[0].trim()));
					  }
					  if (!"NULL".equalsIgnoreCase(parts[1])) {
						  geoInfo.setCountry(parts[1]);					  
					  }
					  if (!"NULL".equalsIgnoreCase(parts[2])) {
						  geoInfo.setRegion(parts[2]);
					  }
					  if (!"NULL".equalsIgnoreCase(parts[3])) {
						  geoInfo.setCity(parts[3]);
					  }
					  if (!"NULL".equalsIgnoreCase(parts[4]) || parts[4].trim().length() > 0) {
						  geoInfo.setLatitude(Float.parseFloat(parts[4].trim()));
					  }
					  if (!"NULL".equalsIgnoreCase(parts[5]) || parts[5].trim().length() > 0) {
						  geoInfo.setLongitude(Float.parseFloat(parts[5].trim()));
					  }
					  if (!"NULL".equalsIgnoreCase(parts[6])) {
						  geoInfo.setMetroCode(parts[6].trim());				  
					  }
			          return geoInfo;		        		
	        	} catch (Throwable exc) {
	        		log ("loading " + citiesPathFileName, exc);
	        		return null;
	        	}
	        }
	      });
	    
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("javaSparkContext.textFile(citiesFileName).map table " + citiesPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

	    startTime = System.currentTimeMillis();
    	DataFrame geoInfoDataFrame = hiveContext.createDataFrame(geoInfoRDD, GeoInfo.class);
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("hiveContext.createDataFrame table " + citiesPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

	    // log ("=== geoInfoDataFrame.count(): " + geoInfoDataFrame.count());		
	    // log ("hiveSchemaipv6_city.count(): " + hiveSchemaipv6_city.count());		
	    if (cacheFlag) {
		    startTime = System.currentTimeMillis();
		    geoInfoDataFrame.cache();		    	
	    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
	    	durMinutes = durSeconds / 60F;
	    	log ("cache table " + citiesPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
	    }
	    
	    startTime = System.currentTimeMillis();
	    geoInfoDataFrame.registerTempTable(citiesFileName + geoDataVersion);
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("registerTempTable table " + citiesPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);
	    
    	ipv6_cityPathFileName = geoSourceFilePath + geoDataVersion + "/" + ipv6_cityFileName + geoDataVersion + ".csv";
	    log("ipv6_cityFileName source: " + ipv6_cityPathFileName);
	    startTime = System.currentTimeMillis();
	    JavaRDD<IPRange> ipv6CityRDD = javaSparkContext.textFile(ipv6_cityPathFileName).map(
	      new Function<String, IPRange>() {
	        @Override
	        public IPRange call(String line) {

	        	try {
		        	  String[] parts = line.split(",");
					  ipv6CityCount++;
					  if ((ipv6CityCount % 100_000) == 1) {
						  log ("ipv6CityCount: " + ipv6CityCount + " - loading targetId: " + parts[0]);					  
					  }

			          IPRange ipRange = new IPRange();
			          ipRange.setTargetId(Integer.parseInt(parts[0].trim()));
			          ipRange.setIpv6_start (parts[1]);  
			          ipRange.setIpv6_end (parts[2]); 
			          return ipRange;		        		
	        	} catch (Throwable exc) {
	        		log ("loading " + ipv6_cityPathFileName, exc);
	        		return null;
	        	}
	        }
	      });
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("javaSparkContext.textFile(citiesFileName).map table " + ipv6_cityPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);

    	if (preloadTableFlag) {
    	    startTime = System.currentTimeMillis();
    	    ipv6CityRDD.count();
        	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
        	durMinutes = durSeconds / 60F;
        	log ("ipv6CityRDD.count table " + ipv6_cityPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);	        		
    	}
	    
	    startTime = System.currentTimeMillis();
	    DataFrame ipv6CityDataFrame = hiveContext.createDataFrame(ipv6CityRDD, IPRange.class);
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("hiveContext.createDataFrame table " + ipv6_cityPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);	    	    
	    
	    if (cacheFlag) {
		    startTime = System.currentTimeMillis();
	    	ipv6CityDataFrame.cache();
	    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
	    	durMinutes = durSeconds / 60F;
	    	log ("cache table " + ipv6_cityPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);	    
	    }
	    
	    startTime = System.currentTimeMillis();
	    ipv6CityDataFrame.registerTempTable(ipv6_cityFileName + geoDataVersion);
    	durSeconds = (float)(System.currentTimeMillis() - startTime) / 1000F;
    	durMinutes = durSeconds / 60F;
    	log ("registerTempTable table " + ipv6_cityPathFileName + " in seconds: " + durSeconds + " / in minutes: " + durMinutes);	    

    	ispsPathFileName = geoSourceFilePath + geoDataVersion + "/" + ispsFileName + geoDataVersion + ".csv";
	    log("ispsFileName source: " + ispsFileName);
	    JavaRDD<IspInfo> ispInfoRDD = javaSparkContext.textFile(ispsPathFileName).map(
	      new Function<String, IspInfo>() {
	        @Override
	        public IspInfo call(String line) {

				IspInfo ispInfo;
	        	try {
		        	  String[] parts = line.split(",");
					  ispsCount++;
					  if ((ispsCount % 10_000) == 1) {
						  log ("# isps loaded: " + ispsCount  + " loading IspId: " + parts[0]);					  
					  }

					  ispInfo = new IspInfo();
					  if (!"NULL".equalsIgnoreCase(parts[0]) || parts[0].trim().length() > 0) {
						  ispInfo.setIspId(Integer.parseInt(parts[0].trim()));
						  if (!"NULL".equalsIgnoreCase(parts[1])) {
							  ispInfo.setProviderName((parts[1]));					  
						  }
				          return ispInfo;
					  }
					  return null;		        		
	        	} catch (Throwable exc) {
	        		log ("loading " + ispsPathFileName, exc);
	        		return null;
	        	}
	        }
	      });		    
	    log("ispInfoRDD createDataFrame");
	    DataFrame ispInfoDataFrame = hiveContext.createDataFrame(ispInfoRDD, IspInfo.class);
	    // log ("=== ispInfoDataFrame.count(): " + ispInfoDataFrame.count());		
	    if (cacheFlag) {
		    log ("=== ispInfoDataFrame.cache()");		
	    	ispInfoDataFrame.cache();
	    }
	    log ("=== ispInfoDataFrame.registerTempTable for isps ");		
	    ispInfoDataFrame.registerTempTable(ispsFileName + geoDataVersion);
	    
	    ipv6_ispPathFileName = geoSourceFilePath + geoDataVersion + "/" + ipv6_ispFileName + geoDataVersion + ".csv";
	    log("ipv6_ispFileName source: " + ipv6_ispFileName);
	    JavaRDD<IPRange> ipv6IspRDD = javaSparkContext.textFile(ipv6_ispPathFileName).map(
	      new Function<String, IPRange>() {
	        @Override
	        public IPRange call(String line) {

	        	try {
		        	  String[] parts = line.split(",");
					  ipv6IspCount++;
					  if ((ipv6IspCount % 100_000) == 1) {
						  log ("ipv6IspCount: " + ipv6IspCount + " - loading targetId: " + parts[0]);					  
					  }

			          IPRange ipRange = new IPRange();
			          ipRange.setTargetId(Integer.parseInt(parts[0].trim()));
			          ipRange.setIpv6_start (parts[1]);  
			          ipRange.setIpv6_end (parts[2]); // parts[1].getBytes(StandardCharsets.UTF_8));
			          return ipRange;		        		
	        	} catch (Throwable exc) {
	        		log ("loading " + ipv6_ispPathFileName, exc);
	        		return null;
	        	}
	        }
	      });  
	    
	    log ("ipv6_isp createDataFrame");
	    DataFrame ipv6IspDataFrame = hiveContext.createDataFrame(ipv6IspRDD, IPRange.class);
	    // log ("=== hiveSchemaipv6_city.count(): " + hiveSchemaipv6_city.count());		
	    if (cacheFlag) {
	    	log ("ipv6IspDataFrame.cache()");		
	    	ipv6IspDataFrame.cache(); // hiveContext.uncacheTable(tableName);
	    }
	    log ("registerTempTable ipv6IspDataFrame");		
	    ipv6IspDataFrame.registerTempTable(ipv6_ispFileName + geoDataVersion);	    
    	*/
    }

    private static SimpleDateFormat format = new SimpleDateFormat("dd MMM yyyy HH:mm:ss.SSS z");
    private static Date date = new Date (Calendar.getInstance().getTimeInMillis());
    
    private static void log (String msg) {
    	date.setTime(Calendar.getInstance().getTimeInMillis());
	    System.out.println (format.format(date) + ": " + msg);
	    logger.debug(msg);    	
    }
    
    private static void log (String msg, Throwable exc) {
	    log (msg + " - " + exc.getLocalizedMessage());
	    exc.printStackTrace();
	    logger.error(exc.getLocalizedMessage(), exc);    	
    }
    
}
