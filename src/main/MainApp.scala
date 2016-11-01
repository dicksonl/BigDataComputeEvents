package main

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
  data.cache
  var collection = data.where("mobilestatus = ?", "Speed Violation").collect
  var sixTill9Am = collection.filter(x => Helpers.getRowsByHourRange(x, 6, 9)).length
  var nineTo12 = collection.filter(x => Helpers.getRowsByHourRange(x, 9, 12)).length
  var twelveTo3pm = collection.filter(x => Helpers.getRowsByHourRange(x, 12, 15)).length
  var threeTo6pm = collection.filter(x => Helpers.getRowsByHourRange(x, 15, 18)).length
  var sixPmTo9pm = collection.filter(x => Helpers.getRowsByHourRange(x, 18, 21)).length
  var ninePmToMidnight = collection.filter(x => Helpers.getRowsByHourRange(x, 21, 0)).length

  System.out.println("sixTill9Am "+ sixTill9Am)
  System.out.println("nineTo12 " + nineTo12)
  System.out.println("twelveTo3pm " + twelveTo3pm)
  System.out.println("threeTo6pm " + threeTo6pm)
  System.out.println("sixPmTo9pm " + sixPmTo9pm)
  System.out.println("ninePmToMidnight " + ninePmToMidnight)
}
