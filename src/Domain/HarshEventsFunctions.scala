package Domain

import Data.CassandraContext
import Models.harsheventsbystreet
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object HarshEventsFunctions {
  def ByStreet(data: CassandraTableScanRDD[CassandraRow]){
      val harshAcc = data
        .where("mobilestatus = ?", "Harsh Acceleration")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)

      val harshBraking = data
        .where("mobilestatus = ?", "Harsh Braking")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)

      val harshCornering = data
        .where("mobilestatus = ?", "Harsh Cornering")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)

      val safeDriving = data
        .where("mobilestatus = ?", "Driving")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)

      val totalEvents = harshAcc
        .union(harshBraking)
        .union(harshCornering)
        .groupByKey()
        .mapValues(x => x.sum)

      val totalMovements = totalEvents
        .union(safeDriving)
        .groupByKey()
        .mapValues(x => x.sum)

      var probability = mutable.Map[
        String,
        ((Float, Int, Float),
         (Float, Int, Float),
         (Float, Int, Float),
          Int)]()

       val accMap = harshAcc.collectAsMap
       val brkMap = harshBraking.collectAsMap
       val crnMap = harshCornering.collectAsMap

      totalMovements.collect.foreach(
        t => {
        var accProb = (0f, 0, 0f)
        var brakingProb = (0f, 0, 0f)
        var corneringProb = (0f, 0, 0f)

        breakable {
          accMap.foreach(a =>{
            if(a._1.getString("street") == t._1.getString("street")){
              accProb = (a._2.toFloat / t._2, a._2, (a._2.toFloat/t._2)*a._2)
              break
            }})
        }

        breakable {
          brkMap.foreach(b =>{
            if(b._1.getString("street") == t._1.getString("street")){
              brakingProb = (b._2.toFloat / t._2, b._2, (b._2.toFloat/t._2)*b._2)
              break
            }})
        }

        breakable {
          crnMap.foreach(c =>{
            if(c._1.getString("street") == t._1.getString("street")){
              corneringProb = (c._2.toFloat / t._2, c._2, (c._2.toFloat/t._2)*c._2)
              break
            }})
        }

        probability += ((t._1.getString("street"), (accProb, brakingProb, corneringProb, t._2)))
      })

     CassandraContext.StoreEventsForStreets(probability)
  }


  def GetStreetRanking(data: CassandraTableScanRDD[CassandraRow]) {
    val collection = data.where("totalmovements > ?", 0).collect
    var rank = new ListBuffer[harsheventsbystreet]

    collection.foreach( x =>
      rank +=  new harsheventsbystreet(
        x.getString("address"),
        x.getInt("accevents"),
        x.getFloat("accpercent"),
        x.getFloat("accsignificance"),
        x.getInt("brkevents"),
        x.getFloat("brkpercent"),
        x.getFloat("brksignificance"),
        x.getInt("crnevents"),
        x.getFloat("crnpercent"),
        x.getFloat("crnsignificance"),
        x.getInt("totalmovements"))
    )

    val sorted = rank.sortBy(_.accsignificance).reverse.take(100)
    sorted.foreach(x => System.out.println(x.address + ":" + x.accsignificance + "=" + x.accevents + "/" + x.totalmovements))
  }
}
