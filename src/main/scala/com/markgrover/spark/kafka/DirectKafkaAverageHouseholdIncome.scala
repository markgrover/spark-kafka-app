package com.markgrover.spark.kafka

import scala.collection.mutable
import kafka.serializer.StringDecoder
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

object DirectKafkaAverageHouseholdIncome {

  object Mode extends Enumeration {
    type mode = Value
    val KAFKA, TEXT = Value
  }

  object KafkaClientVersion extends Enumeration {
    type version = Value
    val V08, V09 = Value
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.toString)
    val ssc = new StreamingContext(conf, Seconds(1))
    val hdfsPath = "/user/hive/warehouse/income"
    // Use a mutable map because the value of one of the properties depends on what version of Kafka
    // client API is being used.
    val kafkaParams: mutable.Map[String, String] = new mutable.HashMap[String, String]()
    // We are not adding 'auto.offset.reset' just yet because its value depends on whate version
    // of Kafka client API is being used.
    kafkaParams.update("bootstrap.servers", "mgrover-st-1.vpc.cloudera.com:9092")
    kafkaParams.update("key.deserializer", "org.apache.kafka.common.serialization" +
      ".StringDeserializer")
    kafkaParams.update("value.deserializer", "org.apache.kafka.common.serialization" +
      ".StringDeserializer")

    val (mode: Mode.Value, clientVersion: KafkaClientVersion.Value) = parseCommandLineArgs(args)

    val incomeCsv: DStream[(String, String)] = mode match {
      case Mode.KAFKA => createVersionSpecificDirectStream(ssc, kafkaParams, clientVersion)
      case Mode.TEXT => ssc.fileStream[LongWritable, Text, TextInputFormat](hdfsPath).map(kv =>
        (kv._1.toString, kv._2.toString))
    }

    val areaIncomeStream = parse(incomeCsv)

    // First element of the tuple in DStream are total incomes, second is total number of zip codes
    // in that geographic area, for which the income is shown
    val runningTotals = areaIncomeStream.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(divide)

    runningTotals.print(20)

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * Parses the command lines arguments and returns the mode (kafka vs. text) and if Kafka
   * the Kafka client version to use.
   * @param args
   * @return
   */
  def parseCommandLineArgs (args: Array[String]): (Mode.Value, KafkaClientVersion.Value) = {
    // Default mode is Kafka
    var mode = Mode.KAFKA
    // Default client version we are going to use v09.
    var clientVersion = KafkaClientVersion.V09

    // If the optional first argument is passed, that represents the mode (case-insensitive)
    if (args.length > 0) {
      val arg = args(0)
      try {
        mode = Mode.withName(arg.toUpperCase())
      } catch {
        case e: NoSuchElementException => throw new scala.IllegalArgumentException(s"Unknown " +
          s"mode (${arg})detected. Available modes are ${Mode.values.mkString(", ")}.")
      }
    }

    // If the optional second argument is passed, that represents the version of Kafka Client
    // API to
    // use. If Kafka mode is not used, the second argument is simply ignored.
    if ((mode == Mode.KAFKA) && (args.length > 1)) {
      val arg = args(1)
      try {
        clientVersion = KafkaClientVersion.withName(arg.toUpperCase())
      } catch {
        case e: NoSuchElementException => throw new scala.IllegalArgumentException("Unknown " +
          s"kafka client version (${arg})detected. Available kafka client versions are " +
          s"${KafkaClientVersion.values.mkString(", ")}.")
      }
    }
    (mode, clientVersion)
  }

  /**
   * Unfortunately, the acceptable values for 'auto.offset.reset' have changed between v08 and v09.
   * v08 accepted 'smallest'/'largest', v09 accepts 'earliest'/'latest'
   * @param ssc Streaming context
   * @param kafkaParams Map contains the configuration parameters for Kafka
   * @param clientVersion @KafkaClientVersion enumeration used for determining what Kafka Client
   *                      API version to use.
   * @return Raw input DStream read from Kafka
   */
  def createVersionSpecificDirectStream(ssc: StreamingContext, kafkaParams: mutable.Map[String,
    String], clientVersion:
  KafkaClientVersion.Value): InputDStream[(String, String)] = {
    if (clientVersion == KafkaClientVersion.V09) {
      kafkaParams.update("auto.offset.reset", "earliest")
      org.apache.spark.streaming.kafka.v09.KafkaUtils.createDirectStream[String, String](ssc,
        kafkaParams.toMap, Set("income"))

    } else if (clientVersion == KafkaClientVersion.V08) {
      kafkaParams.update("auto.offset.reset", "smallest")
      org.apache.spark.streaming.kafka.KafkaUtils.createDirectStream[String, String,
        StringDecoder, StringDecoder](ssc, kafkaParams.toMap, Set("income"))
    } else {
      throw new UnsupportedOperationException(s"client version ($clientVersion) is unsupported.")
    }
  }

  def divide(xy: (Int, Int)): Double = {
    if (xy._2 == 0) {
      0
    } else {
      xy._1 / xy._2
    }

  }

  /**
   * Parse the input DStream of tuples into a new output DStream of tuples.
   * The first element of the input DStream's tuple is the row number, as set by the input source
   * (like TextStream or Kafka) and is ignored. The second element of the input DStream's tuple
   * is the actual record. This record is in CSV format, read as-it-is from the accompanying data
   * file with this code repo.
   * The first element of output tuple is a geographic region which is represented by the first 3
   * digits of the zip code. The second is the income of a subset within that region. This subset is
   * represented by an exact 5 digit zipcode. However, we prune the zipcode down to 3 digits in the
   * output so we can do aggregations based on 3-digit regions.
   * Format of the data is
   * GEO.id,GEO.id2,GEO.display-label,VD01
   * Id,Id2,Geography,Median family income in 1999
   * Example row:
   * 8600000US998HH,998HH,"998HH 5-Digit ZCTA, 998 3-Digit ZCTA",0
   *
   * @param incomeCsv
   * @return
   */
  def parse(incomeCsv: DStream[(String, String)]): DStream[(String, Int)] = {
    val builder = StringBuilder.newBuilder
    val parsedCsv: DStream[List[String]] = incomeCsv.map(entry => {
      val x = entry._2
      var result = List[String]()
      var withinQuotes = false
      x.foreach(c => {
        if (c.equals(',') && !withinQuotes) {
          result = result :+ builder.toString
          builder.clear()
        } else if (c.equals('\"')) {
          builder.append(c)
          withinQuotes = !withinQuotes
        } else {
          builder.append(c)
        }
      })
      result :+ builder.toString
    })
    // 2nd element (index 1) is zip code, last element (index 3) is income
    // We take the first 3 digits of zip code and find average income in that geographic area
    parsedCsv.map(record => (record(1).substring(0, 3), record(3).toInt))

  }
}
