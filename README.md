# spark-kafka-app
### Compile
````mvn clean package````

### HDFS setup
<pre>
<code>
sudo -u hdfs hadoop fs -mkdir -p /user/hive/warehouse/income
sudo -u hdfs hadoop fs -mkdir -p /user/$USER || :
</code>
</pre>

### Run
````spark-submit --master yarn --deploy-mode client --class com.markgrover.spark.kafka.DirectKafkaAverageHouseholdIncome spark-kafka-app_2.10-0.1.0-SNAPSHOT.jar````

### To "stream" the data from HDFS once the app is running
````
# Test
cp ./DEC_00_SF3_P077_with_ann.csv /tmp
sudo -u hdfs hadoop fs -put /tmp/DEC_00_SF3_P077_with_ann.csv /tmp
# This does an atomic move operation which is a requirement for getting Spark Streaming to read data off HDFS as a stream
sudo -u hdfs hadoop fs -mv /tmp/DEC_00_SF3_P077_with_ann.csv /user/hive/warehouse/income
````


