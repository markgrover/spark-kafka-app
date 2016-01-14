# spark-kafka-app
### Pre-requisites
For this application, you'd need a Kafka cluster (which in turn requires zookeeper), and a Hadoop cluster with Spark installed on it.

### Compile
````mvn clean package````

### HDFS setup
<pre>
<code>
sudo -u hdfs hadoop fs -mkdir -p /user/hive/warehouse/income
sudo -u hdfs hadoop fs -mkdir -p /user/$USER || :
sudo -u hdfs hadoop fs -chown -R $USER: /user/$USER
</code>
</pre>

### Run
<pre>
<code>
spark-submit --master yarn --deploy-mode client --class \
com.markgrover.spark.kafka.DirectKafkaAverageHouseholdIncome spark-kafka-app_2.10-0.1.0-SNAPSHOT.jar [kafka|text]
</code>
</pre>

The last optional parameter ```[kafka|text]``` symbolizes whether you want to read data from Kafka or from a text file. Below we should how to populate and run both such sources.

#### Option 1: To use Kafka as a streaming source
You have to create a topic in Kafka named income and put all the CSV data in it. Log on to one of the nodes of the Kafka cluster and run the following instructions.

Note: The binaries named ```kafka-topics```, ```kafka-console-producer``` etc. are valid and present in your path when using CDH. If you are using a non-CDH distribution, you should change them to ```$KAFKA_HOME/bin/kafka-topics.sh```, ```kafka-console-producer.sh``` and so on, respectively.

First list all the topics:

```kafka-topics --list --zookeeper localhost:2181```

Then, create new a topic named ```income```. The replication factor of 3 adds fault tolerance and a non-unit (4, in our case) number of partitions allows us to have upto 4 executors read data directly off of kafka partitions when using the [DirectKafkaInputDStream](https://github.com/apache/spark/blob/master/external/kafka/src/main/scala/org/apache/spark/streaming/kafka/DirectKafkaInputDStream.scala)

```kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 4 --topic income```

Then, put some data in the newly created ```income``` topic.

```cat ./DEC_00_SF3_P077_with_ann.csv | kafka-console-producer --broker-list localhost:9092 --topic income```

You can verify that the data has been by launching a console consumer. It's ok, you can re-read the data, our kafka app, always reads the data from the beginning:
```kafka-console-consumer --zookeeper localhost:2181 --topic income --from-beginning```

#### Option 2: To use a text file as a streaming source
Log on to one of the nodes of the Hadoop cluster and run the following instructions to create a stream out of HDFS data. Start your streaming application *before* you run the steps below.

Put the data file into HDFS's /tmp directory.

```
cp ./DEC_00_SF3_P077_with_ann.csv /tmp
sudo -u hdfs hadoop fs -put /tmp/DEC_00_SF3_P077_with_ann.csv /tmp
```

This is required because we need to an atomic move operation to the HDFS directory (```/user/hive/warehouse/income```) which our streaming application is listening to. Putting the file directly from the local filesystem into the destination directly won't be atomic and would confuse our spark streaming application.

```
sudo -u hdfs hadoop fs -mv /tmp/DEC_00_SF3_P077_with_ann.csv /user/hive/warehouse/income
```

### Verifying that your streaming application is working
Search for ```Time: ``` and you should see a snippet like this:

```
-------------------------------------------
Time: 1452799840000 ms
-------------------------------------------
(006,13530.0)
(007,13419.0)
```
This is our average income for the regions in the data set. This shows that region 006 (i.e. all zipcodes starting with 006) have an average annual income of $13,530 and of $13,419. for those in region 007.
