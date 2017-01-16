package iosr.lambda.arch

import org.apache.spark.{SparkConf, SparkContext}

object AviationStatistics {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IOSR Lambbda Architecture POC")
    conf.set("log4j.rootcategory", "ERROR, console")
    val sc = new SparkContext(conf)
    val inputFiles = "/home/tlegutko/aviation/airline_origin_destination/*/*.csv" // 74GB
    val outputFileName = "src/main/resources/results"
    val originAirportCode = "Origin"
    val destinationAirportCode = "Dest"
    val file = sc.textFile(inputFiles)
    val header = file.first().replaceAll("\"", "").split(",")
    val originAirportIndex = header.indexOf(originAirportCode)
    val destinationAirportIndex = header.indexOf(destinationAirportCode)
    val body = file.filter(_ != header)
    val result = body.flatMap(line => {
      val fields = line.replaceAll("\"", "").split(",")
      Array((fields(originAirportIndex), 1),
        (fields(destinationAirportIndex), 1))
    }).reduceByKey(_ + _).sortBy(-_._2)
    result.collect().foreach(println) // TODO either save collected info to file or merge hdfs output
  }


}
