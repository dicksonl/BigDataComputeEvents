package main

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD

object MainApp extends App{
  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Spark Count")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
  )

  var data = sc.cassandraTable("ctrack", "trips")
  var rows = data.toJavaRDD()

  System.out.println("Spark job complete")
}
