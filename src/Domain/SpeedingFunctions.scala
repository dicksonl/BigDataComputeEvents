package Domain

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import main.Helpers
import Data.CassandraContext

import scala.collection.mutable
import Models.{SpeedingByDriverDistance, speeding}
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object SpeedingFunctions {
  def ByHour(data: CassandraTableScanRDD[CassandraRow]){
    val collection = data.where("mobilestatus = ?", "Speed Violation").collect

    val h6 = collection.count(x => Helpers.getRowsByHourRange(x, 6, 7))
    val h7 = collection.count(x => Helpers.getRowsByHourRange(x, 7, 8))
    val h8 = collection.count(x => Helpers.getRowsByHourRange(x, 8, 9))
    val h9 = collection.count(x => Helpers.getRowsByHourRange(x, 9, 10))
    val h10 = collection.count(x => Helpers.getRowsByHourRange(x, 10, 11))
    val h11 = collection.count(x => Helpers.getRowsByHourRange(x, 11, 12))
    val h12 = collection.count(x => Helpers.getRowsByHourRange(x, 12, 13))
    val h13 = collection.count(x => Helpers.getRowsByHourRange(x, 13, 14))
    val h14 = collection.count(x => Helpers.getRowsByHourRange(x, 14, 15))
    val h15 = collection.count(x => Helpers.getRowsByHourRange(x, 15, 16))
    val h16 = collection.count(x => Helpers.getRowsByHourRange(x, 16, 17))
    val h17 = collection.count(x => Helpers.getRowsByHourRange(x, 17, 18))
    val h18 = collection.count(x => Helpers.getRowsByHourRange(x, 18, 19))
    val h19 = collection.count(x => Helpers.getRowsByHourRange(x, 19, 20))
    val h20 = collection.count(x => Helpers.getRowsByHourRange(x, 20, 21))
    val h21 = collection.count(x => Helpers.getRowsByHourRange(x, 21, 22))
    val h22 = collection.filter(x => Helpers.getRowsByHourRange(x, 22, 23)).length

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

  def ByStreet(data: CassandraTableScanRDD[CassandraRow]) {
    val speeding =
      data.where("mobilestatus = ?", "Speed Violation").select("street").map(x =>(x, 1)).reduceByKey(_+_).collect
    val safeDriving =
      data.where("mobilestatus = ?", "Driving").select("street").map(x =>(x, 1)).reduceByKey(_+_).collect
    val totalMovements = ( speeding ++ safeDriving ).groupBy( _._1 ).map( kv => (kv._1, kv._2.map( _._2).sum ))
    var probability = mutable.Map[String,(Float, Int, Int, Float)]()
    totalMovements.foreach( t => {
      var lProb = (0f, 0, 0, 0f)
      breakable {
        speeding.foreach(s =>{
          if(s._1.getString("street") == t._1.getString("street")){
              lProb = (s._2.toFloat / t._2, s._2, t._2, (s._2.toFloat/t._2)*s._2)
              break
          }
        })
      }
       probability += ((t._1.getString("street"), lProb))
    })
    CassandraContext.StoreSpeedingForStreets(probability)
    System.out.println("Completed By Street")
  }

  def ByDriverSpeedDistanceRanking(data: CassandraTableScanRDD[CassandraRow]): Unit ={
    val speeding =
      data.where("mobilestatus = ?", "Speed Violation").map(x =>(x.getString("driverid"), x.getInt("mobileodo"))).reduceByKey(_+_)
    val safeDriving =
      data.where("mobilestatus = ?", "Driving").map(x =>(x.getString("driverid"), x.getInt("mobileodo"))).reduceByKey(_+_)
    val totalMovements = speeding.union(safeDriving).groupByKey().mapValues(x => x.sum)
    val speedingArr = speeding.collect
    var probability = mutable.Map[String,(Float, Int, Int, Float)]()
    totalMovements.collect.foreach( t => {
      var lProb = (0f, 0, 0, 0f)
      breakable {
        speedingArr.foreach(s => {
          if (s._1 == t._1) {
            lProb = (s._2.toFloat / t._2, s._2, t._2, (s._2.toFloat / t._2) * s._2)
            break
          }
        })
      }
      probability += ((t._1, lProb))
    })
    CassandraContext.StoreSpeedingDistanceForDrivers(probability)
    System.out.println("Completed By driver speed distance")
  }

  def ByDriverDangerousDriving(data: CassandraTableScanRDD[CassandraRow]) {
    val speeding =
      data.where("mobilestatus = ?",
                 "Speed Violation")
        .map(x =>(
          x.getString("driverid"),
          x.getInt("mobilespeed"),
          x.getString("street"),
          x.getFloat("streetmaxspeed"),
          x.getDateTime("ddate")))
        .collect

    var dangerousSpeed =
      new ListBuffer[(String, Int, String, Float, DateTime)]

    speeding.foreach(x => {
      if(x._2/x._4 >= 1.2){
        dangerousSpeed += x
      }
    })
    

//
//    dangerousSpeed.sortBy(_._2).reverse.foreach(x => {
//      System.out.println(x._1 + " at " + x._3 + " speeding over " + (((x._2/x._4)*100) - 100) + "% doing " + x._2 + " on a " + x._4 + " road")
//    })
  }

  def GetStreetRanking(data: CassandraTableScanRDD[CassandraRow]) {
    val collection = data.where("significance > ?", 0).collect
    var rank = new ListBuffer[speeding]

    collection.foreach( x =>
      rank +=  new speeding(
        x.getString("address"),
        x.getFloat("significance"),
        x.getInt("speedingcount"),
        x.getFloat("speedingpercent"),
        x.getInt("totalmovement"))
    )

    val sorted = rank.sortBy(_.significance).reverse
    sorted.foreach(x => System.out.println(x.address + ":" + x.significance + "=" + x.speedingcount + "/" + x.totalmovement))
  }

  def GetDriverDistanceRanking(data: CassandraTableScanRDD[CassandraRow]) {
    val collection = data.where("significance > ?", 0).collect
    var rank = new ListBuffer[SpeedingByDriverDistance]

    collection.foreach( x =>
      rank +=  new SpeedingByDriverDistance(
        x.getString("driverid"),
        x.getFloat("significance"),
        x.getInt("speedingmeters"),
        x.getFloat("speedingpercent"),
        x.getInt("totaldrivenmeters"))
    )

    val sorted = rank.sortBy(_.significance).reverse
    sorted.foreach(x => System.out.println(x.driverid + ":" + x.significance + "=" + (x.speedingdistance/1000) + "Km/" + (x.totaldrivendistance / 1000) + "Km"))
  }
}
