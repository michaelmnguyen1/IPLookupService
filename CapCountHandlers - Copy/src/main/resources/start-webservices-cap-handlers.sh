clear

# java -Xmx3g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:MaxGCPauseMillis=1 -jar cap-count-handlers-webservices.jar cap-count-handlers.conf.qa.sparkMaster client-meta-data.conf -Dlog4j.configurationFile=./log4j-api-app.xml

java -Dlog4j.configuration=file:/cakeapps/cap-count-handlers/log4j-webservices-cap-handlers.xml -Xmx3g -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:MaxGCPauseMillis=1 -cp webservices-cap-count-handlers.jar:/cakeapps/cap-count-handlers/cake-jars/*:/cakeapps/spark/lib/spark-assembly-1.6.0-hadoop2.6.0.jar com.getcake.capcount.services.CapCountWebService cap-init-req-count-handlers.conf.qa.sparkMaster client-meta-data.conf 
