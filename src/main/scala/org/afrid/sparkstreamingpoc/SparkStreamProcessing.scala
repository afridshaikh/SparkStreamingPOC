package org.afrid.sparkstreamingpoc

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j
import org.slf4j.LoggerFactory

/**
 * Spark Stream Processing
 *
 */
object SparkStreamProcessing {

  def main(args: Array[String]): Unit = {

    val logger:slf4j.Logger = LoggerFactory.getLogger("App")

    val masterUrl:String = if(System.getenv("master") == null )  "local[*]" else System.getenv("master")

    val sparkSession = SparkSession.builder().appName("sparkstreamingpoc").master(masterUrl).getOrCreate();
    sparkSession.sparkContext.setLogLevel("ERROR")


    val sparkStreamingContex = new StreamingContext(sparkSession.sparkContext, Seconds(15))

    val lineStream = sparkStreamingContex.textFileStream("file:///C:/Users/afrid/Downloads/data")

    val groupedData = lineStream.map{
      row =>
        var splitted = row.split(",")
        val year = splitted(0).split('-')(0)
        val volume = splitted(splitted.length-2).toFloat
        (year,volume)
    }.groupByKey().count()

    groupedData.print()

    sparkStreamingContex.start()
    sparkStreamingContex.awaitTermination()

  }

}
