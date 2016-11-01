package Domain

import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import main.Helpers
import org.apache.spark.SparkContext._

object Speeding {
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
    val speedingCollection =
      data.where("mobilestatus = ?", "Speed Violation").select("street").map(x =>(x, 1)).reduceByKey(_+_).collect
    val safeDrivingCollection =
      data.where("mobilestatus = ?", "Driving").select("street").map(x =>(x, 1)).reduceByKey(_+_).collect


  }
}
