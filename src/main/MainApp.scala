package main

import Domain.Speeding
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._


object MainApp extends App{
  val sc = new SparkContext(
    new SparkConf()
      .setAppName("Spark Count")
      .setMaster("local[4]")
      .set("spark.executor.memory", "2g")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.cassandra.connection.port", "9042")
  )

  var data = sc.cassandraTable("ctrack", "movements")
//    var data = sc.cassandraTable("ctrack", "speedingbystreet")
    data.cache
  //Speeding.ByHour(data)
  Speeding.ByStreet(data)
//    Speeding.GetStreetRanking(data)
}
