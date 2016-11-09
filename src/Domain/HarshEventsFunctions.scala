package Domain

import Data.CassandraContext
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraTableScanRDD

import scala.collection.mutable

object HarshEventsFunctions {
  def ByStreet(data: CassandraTableScanRDD[CassandraRow]){
      val harshAcc = data
        .where("mobilestatus = ?", "Harsh Acceleration")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)
        .collect

      val harshBraking = data
        .where("mobilestatus = ?", "Harsh Braking")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)
        .collect

      val harshCornering = data
        .where("mobilestatus = ?", "Harsh Cornering")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)
        .collect

      val safeDriving = data
        .where("mobilestatus = ?", "Driving")
        .select("street")
        .map(x =>(x, 1))
        .reduceByKey(_+_)
        .collect

      val totalEvents =
        ( harshAcc ++ harshBraking ++ harshCornering)
          .groupBy( _._1 )
          .map( kv => (kv._1, kv._2.map( _._2).sum ))

      val totalMovements =
        ( totalEvents ++ safeDriving )
          .groupBy( _._1 )
          .map( kv => (kv._1, kv._2.map( _._2).sum ))

      var probability = mutable.Map[
        String,
        ((Float, Int, Int, Float),
         (Float, Int, Float),
         (Float, Int, Float))]()

      totalMovements.foreach( t => {
        var accProb = (0f, 0, 0, 0f)
        var brakingProb = (0f, 0, 0f)
        var corneringProb = (0f, 0, 0f)

        harshAcc.foreach(a =>{
          if(a._1.getString("street") == t._1.getString("street")){
            accProb = (a._2.toFloat / t._2, a._2, t._2, (a._2.toFloat/t._2)*a._2)
          }})

        harshBraking.foreach(b =>{
          if(b._1.getString("street") == t._1.getString("street")){
            brakingProb = (b._2.toFloat / t._2, b._2, (b._2.toFloat/t._2)*b._2)
          }})

        harshCornering.foreach(c =>{
          if(c._1.getString("street") == t._1.getString("street")){
            corneringProb = (c._2.toFloat / t._2, c._2, (c._2.toFloat/t._2)*c._2)
          }})

        probability += ((t._1.getString("street"), (accProb, brakingProb, corneringProb)))
      })

    CassandraContext.StoreEventsForStreets(probability)
  }
}
