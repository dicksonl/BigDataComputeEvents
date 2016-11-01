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
  var h6 = collection.filter(x => Helpers.getRowsByHourRange(x, 6, 7)).length
  var h7 = collection.filter(x => Helpers.getRowsByHourRange(x, 7, 8)).length
  var h8 = collection.filter(x => Helpers.getRowsByHourRange(x, 8, 9)).length
  var h9 = collection.filter(x => Helpers.getRowsByHourRange(x, 9, 10)).length
  var h10 = collection.filter(x => Helpers.getRowsByHourRange(x, 10, 11)).length
  var h11 = collection.filter(x => Helpers.getRowsByHourRange(x, 11, 12)).length
  var h12 = collection.filter(x => Helpers.getRowsByHourRange(x, 12, 13)).length
  var h13 = collection.filter(x => Helpers.getRowsByHourRange(x, 13, 14)).length
  var h14 = collection.filter(x => Helpers.getRowsByHourRange(x, 14, 15)).length
  var h15 = collection.filter(x => Helpers.getRowsByHourRange(x, 15, 16)).length
  var h16 = collection.filter(x => Helpers.getRowsByHourRange(x, 16, 17)).length
  var h17 = collection.filter(x => Helpers.getRowsByHourRange(x, 17, 18)).length
  var h18 = collection.filter(x => Helpers.getRowsByHourRange(x, 18, 19)).length
  var h19 = collection.filter(x => Helpers.getRowsByHourRange(x, 19, 20)).length
  var h20 = collection.filter(x => Helpers.getRowsByHourRange(x, 20, 21)).length
  var h21 = collection.filter(x => Helpers.getRowsByHourRange(x, 21, 22)).length
  var h22 = collection.filter(x => Helpers.getRowsByHourRange(x, 22, 23)).length


  System.out.println("h6 "+ h6)
  System.out.println("h7 "+ h7)
  System.out.println("h8 "+ h8)
  System.out.println("h9 "+ h9)
  System.out.println("h10 "+ h10)
  System.out.println("h11 "+ h11)
  System.out.println("h12 "+ h12)
  System.out.println("h13 "+ h13)
  System.out.println("h14 "+ h14)
  System.out.println("h15 "+ h15)
  System.out.println("h16 "+ h16)
  System.out.println("h17 "+ h17)
  System.out.println("h18 "+ h18)
  System.out.println("h19 "+ h19)
  System.out.println("h20 "+ h20)
  System.out.println("h21 "+ h21)
  System.out.println("h22 "+ h22)

}
