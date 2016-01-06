package com.markgrover.spark.kafka

import org.apache.spark.streaming.dstream._
object DirectKafkaAverageHouseholdIncome {
  def main(args: Array[String]) {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming._

    val conf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.toString)
    val ssc = new StreamingContext(conf, Seconds(1))
    val hdfsPath = "hdfs:///user/hive/warehouse/income/"
    val incomeCsv = ssc.textFileStream(hdfsPath)
    // Format of the data is
    //GEO.id,GEO.id2,GEO.display-label,VD01
    //Id,Id2,Geography,Median family income in 1999
    //8600000US998HH,998HH,"998HH 5-Digit ZCTA, 998 3-Digit ZCTA",0
    val parsedCsv = parse(incomeCsv)
    // 2nd element (index 1) is zip code, last element (index 3) is income
    // We take the first 3 digits of zip code and find average income in that geographic area
    val areaIncomeStream = parsedCsv.map(record => (record(1).substring(0, 3), record(3).toInt))

    // First element of the tuple in DStream are total incomes, second is total number of zip codes
    // in that geographic area, for which the income is shown
    val runningTotals = areaIncomeStream.mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(divide)

    runningTotals.print(20)

    ssc.start()
    ssc.awaitTermination()
  }

  def divide(xy: (Int, Int)): Double = {
    if (xy._2 == 0) {
      0
    } else {
      xy._1/xy._2
    }

  }

  def parse(incomeCsv: DStream[String]): DStream[List[String]] = {
    println("here1")
    val builder = StringBuilder.newBuilder
    println("here2")
    incomeCsv.map(x => {
      println("here3")
      var result = List[String]()
      var withinQuotes = false
      x.foreach(c => {
        println("here4")
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
      println ("REsult just added:" + builder.toString)
      result :+ builder.toString
    })
  }
}
