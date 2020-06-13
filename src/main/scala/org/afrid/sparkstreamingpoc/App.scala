package org.afrid.sparkstreamingpoc

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App {
  def main(args: Array[String]): Unit = {

    val masterUrl:String = if(System.getenv("master") == null )  "local[*]" else System.getenv("master")

    val sparkSession = SparkSession.builder().appName("sparkstreamingpoc").master(masterUrl).getOrCreate();



  }
}
