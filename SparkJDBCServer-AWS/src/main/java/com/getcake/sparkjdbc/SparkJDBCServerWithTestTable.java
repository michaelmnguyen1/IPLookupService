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

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.thriftserver.*; 

public class SparkJDBCServerWithTestTable {
	
	private static final Logger logger = Logger.getLogger(SparkJDBCServerWithTestTable.class);
	
   public static class Person implements Serializable {
    private String name;
    private int age;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }
  }

   public static class IPRange implements Serializable {
	  private byte [] ipv6_start_bin, ipv6_end_bin;
	  private String ipv6_start, ipv6_end;
	  
	  private int location_id;

	    public byte [] getIpv6_start_bin () {
	      return ipv6_start_bin;
	    }
	
	    public byte [] getIpv6_end_bin () {
	      return ipv6_end_bin;
	    }
	
	    /*
	    public void setIpv6_start(byte [] ipv6_start) {
		      this.ipv6_start = ipv6_start;
		    }
		
	    public void setIpv6_end(byte [] ipv6_end) {
	      this.ipv6_end = ipv6_end;
	    }
	    */
	    
	    public String getIpv6_start() {
	      return ipv6_start;
	    }
	
	    public void setIpv6_start(String ipv6_start_str) {	    
	      this.ipv6_start = ipv6_start_str;
	      ipv6_start_bin = hexStringToByteArray (ipv6_start_str); 
	    }
	
	    public String getIpv6_end () {
	      return ipv6_end;
	    }
	
	    public void setIpv6_end(String ipv6_end_str) {
	      this.ipv6_end = ipv6_end_str;
	      ipv6_end_bin = hexStringToByteArray (ipv6_end_str); 
	    }
	    
	    public int getLocation_id() {
	      return location_id;
	    }
	
	    public void setLocation_id(int location_id) {
	      this.location_id = location_id;
	    }
	  }

	public static byte[] hexStringToByteArray(String inputStr) {
		byte[] data; 
		int len;
		try {
			inputStr = inputStr.trim();
		    len = inputStr.length();
		    data = new byte[len / 2];
			// System.out.println ("str:" + inputStr + " len:" + len);
		    for (int i = 0; i < len; i += 2) {
				// System.out.println ("i:" + i);		    		
		    	if (i >= 32) {
					System.out.println ("err i:" + i);		    		
		    	}
		        data[i / 2] = (byte) ((Character.digit(inputStr.charAt(i), 16) << 4)
		                             + Character.digit(inputStr.charAt(i+1), 16));
		    }			
		    return data;
		} catch (Throwable exc) {
			System.out.println ("err str:" + inputStr);
			exc.printStackTrace();
			throw exc;
		}
	}		

    static int ipCount;
	
    public static void main(String[] args) throws Exception {
	
    	String ipFileName, sparkMaster, appName;
    	
	    try {
		  
		    System.out.println("=== SparkJDBCServer v0.6 ===");
		    
		    if (args == null ||  args.length < 2) {
		    	System.out.println ("usage: <sparkUrl> <ip FileName> <appName (Optional)>");
		    	System.exit (-1);
		    }
		    sparkMaster = args[0];
		    ipFileName = args[1];
		    if (args.length == 3) {
		    	appName = args[2];
		    } else {
		    	appName = "Spark-JDBC-Server"; 
		    }
		    
		    SparkConf sparkConf = new SparkConf().setAppName(appName);
		    sparkConf.setMaster(sparkMaster); // spark://ip-10-128-1-78:7077		    
		    sparkConf.set("spark.executor.memory", "7g");
		    sparkConf.set("spark.driver.memory", "9g");
		    
		    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		    SQLContext sqlContext = new SQLContext(ctx.sc());
		    HiveContext hiveContext = new HiveContext (ctx.sc());

		    System.out.println("=== Data source: RDD loading ./examples/src/main/resources/people.txt ===");
		    // Load a text file and convert each line to a Java Bean.
		 	// JavaRDD<Person> people = ctx.textFile("src/main/resources/people.txt").map(
		    // 
		    JavaRDD<Person> people = ctx.textFile("./examples/src/main/resources/people.txt").map(
		      new Function<String, Person>() {
		        @Override
		        public Person call(String line) {
		          String[] parts = line.split(",");

		          Person person = new Person();
		          person.setName(parts[0]);
		          person.setAge(Integer.parseInt(parts[1].trim()));

		          return person;
		        }
		      });

		    // Apply a schema to an RDD of Java Beans and register it as a table.
		    DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
		    schemaPeople.registerTempTable("people");
		    
		    // SQL can be run over RDDs that have been registered as tables.
		    DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

		    teenagers.registerTempTable("teenagers");
		    
		    // The results of SQL queries are DataFrames and support all the normal RDD operations.
		    // The columns of a row in the result can be accessed by ordinal.
		    List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
		      @Override
		      public String call(Row row) {
		        return "Name: " + row.getString(0);
		      }
		    }).collect();
		    for (String name: teenagerNames) {
		      System.out.println(name);
		    }

		    // DataFrame hiveSchemaPeople = hiveContext.createDataFrame(people, Person.class);
		    // hiveSchemaPeople.registerTempTable("peopleHive");

		    // SQL can be run over RDDs that have been registered as tables.
		    /*
		    teenagers = hiveContext.sql("SELECT name FROM peopleHive WHERE age >= 13 AND age <= 19");

		    teenagers.registerTempTable("teenagers");
		    
		    // The results of SQL queries are DataFrames and support all the normal RDD operations.
		    // The columns of a row in the result can be accessed by ordinal.
		    teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
		      @Override
		      public String call(Row row) {
		        return "Teenager Name: " + row.getString(0);
		      }
		    }).collect();
		    for (String name: teenagerNames) {
		      System.out.println(name);
		    }
		    */
		    
		    
		    // "C:/Projects/GeoServices/ip-data.csv"
		    System.out.println("=== Data source: RDD loading " + ipFileName);
		    JavaRDD<IPRange> ipv6_city_A = ctx.textFile(ipFileName).map(
		      new Function<String, IPRange>() {
		        @Override
		        public IPRange call(String line) {

	        	  String[] parts = line.split(",");
				  ipCount++;
				  if ((ipCount % 100_000) == 0) {
					  System.out.println ("# ip loaded: " + ipCount + " - loading ipv6_start: " + parts[0]);					  
				  }

		          IPRange ipRange = new IPRange();
		          /*
		          ipRange.setIpv6_start(parts[0]);
		          ipRange.setIpv6_end (parts[1]);
		          */
		          ipRange.setIpv6_start (parts[0]); // 
		          // ipRange.setIpv6_start(hexStringToByteArray (parts[0])); // 
		          // System.out.println(ipRange.getIpv6_start());
		          byte [] bytes = parts[0].getBytes(StandardCharsets.UTF_8);          
		          // System.out.println(bytes);

		          // ipRange.setIpv6_end (hexStringToByteArray (parts[1])); // parts[1].getBytes(StandardCharsets.UTF_8));
		          ipRange.setIpv6_end (parts[1]); // parts[1].getBytes(StandardCharsets.UTF_8));
		          // System.out.println(ipRange.getIpv6_end());
		          bytes = parts[1].getBytes(StandardCharsets.UTF_8);          
		          // System.out.println(bytes);

		          ipRange.setLocation_id(Integer.parseInt(parts[2].trim()));

		          return ipRange;
		        }
		      });
		    // System.out.println ("ipv6_city_A.count(): " + ipv6_city_A.count());		    		    
		    
		    DataFrame hiveSchemaipv6_city_A = hiveContext.createDataFrame(ipv6_city_A, IPRange.class);
		    System.out.println ("=== hiveSchemaipv6_city_A.count(): " + hiveSchemaipv6_city_A.count());		
		    hiveSchemaipv6_city_A.cache();
		    System.out.println ("=== hiveSchemaipv6_city_A.cache()");		
		    // System.out.println ("hiveSchemaipv6_city_A.count(): " + hiveSchemaipv6_city_A.count());		
		    hiveSchemaipv6_city_A.registerTempTable("ipv6_city_A");

		    /*
		    DataFrame cities =
		    		hiveContext.sql("SELECT ipv6_start, ipv6_end, location_id  FROM ipv6_city_A"); //  WHERE ipv6_start = 01060000
		    List<String> citiList = cities.toJavaRDD().map(new Function<Row, String>() {
		        @Override
		        public String call(Row row) {
		          return " - location_id: " + row.getInt(2); // "ipv6_start: "  +  + row.g(0) + " - ipv6_end: " + row.getString(1)
		        }
		      }).collect();

		    for (String row: citiList) {
		        System.out.println(row);
		      }    
		    */
		    
		    HiveThriftServer2.startWithContext(hiveContext);
		    
		    if (true) return;
		    
		    hiveContext.sql("CREATE TABLE IF NOT EXISTS testtable1 (value STRING, key INT)");
		    System.out.println("=== SparkJDBCServer v0.2 Created testtable1 ===");
		    hiveContext.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/people.txt' INTO TABLE testtable1");
		    System.out.println("=== SparkJDBCServer v0.2 load data into testtable1 ===");

		    System.out.println("=== SparkJDBCServer v0.2 Exits  registerTempTable for people and teenagers ===");
		    if (true) return;
		    
		    System.out.println("=== Data source: Parquet File ===");
		    // DataFrames can be saved as parquet files, maintaining the schema information.
		    schemaPeople.write().parquet("people.parquet");

		    // Read in the parquet file created above.
		    // Parquet files are self-describing so the schema is preserved.
		    // The result of loading a parquet file is also a DataFrame.
		    
		    /*
		    DataFrame parquetFile = sqlContext.read().parquet("people.parquet");

		    //Parquet files can also be registered as tables and then used in SQL statements.
		    parquetFile.registerTempTable("parquetFile");
		    */
		    DataFrame teenagers2 =
		      sqlContext.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
		    teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {
		      @Override
		      public String call(Row row) {
		          return "Name: " + row.getString(0);
		      }
		    }).collect();
		    for (String name: teenagerNames) {
		      System.out.println(name);
		    }

		    System.out.println("=== Data source: JSON Dataset ===");
		    // A JSON dataset is pointed by path.
		    // The path can be either a single text file or a directory storing text files.
		    String path = "examples/src/main/resources/people.json";
		    // Create a DataFrame from the file(s) pointed by path
		    DataFrame peopleFromJsonFile = sqlContext.read().json(path);

		    // Because the schema of a JSON dataset is automatically inferred, to write queries,
		    // it is better to take a look at what is the schema.
		    peopleFromJsonFile.printSchema();
		    // The schema of people is ...
		    // root
		    //  |-- age: IntegerType
		    //  |-- name: StringType

		    // Register this DataFrame as a table.
		    peopleFromJsonFile.registerTempTable("people");

		    // SQL statements can be run by using the sql methods provided by sqlContext.
		    DataFrame teenagers3 = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

		    // The results of SQL queries are DataFrame and support all the normal RDD operations.
		    // The columns of a row in the result can be accessed by ordinal.
		    teenagerNames = teenagers3.toJavaRDD().map(new Function<Row, String>() {
		      @Override
		      public String call(Row row) { return "Name: " + row.getString(0); }
		    }).collect();
		    for (String name: teenagerNames) {
		      System.out.println(name);
		    }

		    // Alternatively, a DataFrame can be created for a JSON dataset represented by
		    // a RDD[String] storing one JSON object per string.
		    List<String> jsonData = Arrays.asList(
		          "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
		    JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
		    DataFrame peopleFromJsonRDD = sqlContext.read().json(anotherPeopleRDD.rdd());

		    // Take a look at the schema of this new DataFrame.
		    peopleFromJsonRDD.printSchema();
		    // The schema of anotherPeople is ...
		    // root
		    //  |-- address: StructType
		    //  |    |-- city: StringType
		    //  |    |-- state: StringType
		    //  |-- name: StringType

		    peopleFromJsonRDD.registerTempTable("people2");

		    DataFrame peopleWithCity = sqlContext.sql("SELECT name, address.city FROM people2");
		    List<String> nameAndCity = peopleWithCity.toJavaRDD().map(new Function<Row, String>() {
		      @Override
		      public String call(Row row) {
		        return "Name: " + row.getString(0) + ", City: " + row.getString(1);
		      }
		    }).collect();
		    for (String name: nameAndCity) {
		      System.out.println(name);
		    }

		    ctx.stop();
	  
	  } catch (Throwable exc) {
		  exc.printStackTrace();
		  System.out.println (exc.getStackTrace());
	  }
    }
    
    private static void loadPeople (JavaSparkContext ctx) {
	    SQLContext sqlContext = new SQLContext(ctx.sc());

	    logger.debug("=== Data source: RDD loading ./examples/src/main/resources/people.txt ===");
	    // Load a text file and convert each line to a Java Bean.
	 	// JavaRDD<Person> people = ctx.textFile("src/main/resources/people.txt").map(
	    // 
	    JavaRDD<Person> people = ctx.textFile("./examples/src/main/resources/people.txt").map(
	      new Function<String, Person>() {
	        @Override
	        public Person call(String line) {
	          String[] parts = line.split(",");

	          Person person = new Person();
	          person.setName(parts[0]);
	          person.setAge(Integer.parseInt(parts[1].trim()));

	          return person;
	        }
	      });

	    // Apply a schema to an RDD of Java Beans and register it as a table.
	    DataFrame schemaPeople = sqlContext.createDataFrame(people, Person.class);
	    schemaPeople.registerTempTable("people");
	    
	    // SQL can be run over RDDs that have been registered as tables.
	    DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

	    teenagers.registerTempTable("teenagers");
	    
	    // The results of SQL queries are DataFrames and support all the normal RDD operations.
	    // The columns of a row in the result can be accessed by ordinal.
	    List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
	      @Override
	      public String call(Row row) {
	        return "Name: " + row.getString(0);
	      }
	    }).collect();
	    for (String name: teenagerNames) {
	      logger.debug(name);
	    }
    	
	    // DataFrame hiveSchemaPeople = hiveContext.createDataFrame(people, Person.class);
	    // hiveSchemaPeople.registerTempTable("peopleHive");

	    // SQL can be run over RDDs that have been registered as tables.
	    /*
	    teenagers = hiveContext.sql("SELECT name FROM peopleHive WHERE age >= 13 AND age <= 19");

	    teenagers.registerTempTable("teenagers");
	    
	    // The results of SQL queries are DataFrames and support all the normal RDD operations.
	    // The columns of a row in the result can be accessed by ordinal.
	    teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
	      @Override
	      public String call(Row row) {
	        return "Teenager Name: " + row.getString(0);
	      }
	    }).collect();
	    for (String name: teenagerNames) {
	      logger.debug(name);
	    }
	    */
	    		    
    }
        
}
