clear
export PATH=/home/ec2-user/jdk/bin:$PATH:$HOME/bin
export JAVA_HOME=/home/ec2-user/jdk

/cakeapps/spark/bin/spark-submit --conf spark.root.logger=ALL,FILE --jars ./cake-jars/guava-11.0.2.jar,./cake-jars/aws-java-sdk-core-1.10.16.jar,./cake-jars/aws-java-sdk-s3-1.10.16.jar,./cake-jars/hadoop-aws-2.6.0.jar,./cake-jars/jetty-server-9.0.2.v20130417.jar,./cake-jars/jetty-util-9.0.2.v20130417.jar,./cake-jars/jetty-http-9.0.2.v20130417.jar,./cake-jars/jetty-io-9.0.2.v20130417.jar,./cake-jars/spark-csv_2.11-1.2.0.jar,./cake-jars/commons-csv-1.1.jar,./cake-jars/jtds-1.3.1.jar,./cake-jars/joda-time-2.8.1.jar --properties-file cake-spark-jdbc-server.conf.qa.s3.sparkMaster  --class com.getcake.sparkjdbc.SparkJDBCServer /cakeapps/cake-spark-jdbc-server/cake-spark-jdbc-server.jar cake-spark-jdbc-server.conf.qa.s3.sparkMaster > cake-jdbc-server.log &

