package Data

import com.datastax.driver.core.{PoolingOptions, _}
import org.apache.commons.lang.StringEscapeUtils
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

object CassandraContext {
  val pool = new PoolingOptions()
  val cluster = Cluster.builder()
    .addContactPoint("localhost")
    .withPoolingOptions(pool)
    .build();
  val session = cluster.connect("ctrack")

  def StoreSpeedingForStreets(map : scala.collection.mutable.Map[String,(Float, Int, Int, Float)]): Boolean ={
    try {
      session.execute("Truncate speedingbystreet;")

      val batch = new BatchStatement()

      for((k, v) <- map){
        batch.add(
          new SimpleStatement(
          "INSERT INTO ctrack.speedingbystreet (" +
          "partkey, " +
          "address, " +
          "significance, " +
          "speedingpercent, " +
          "speedingcount, " +
          "totalMovement" +
          ") VALUES (" +
          "1, " +
          "$$" + StringEscapeUtils.escapeJava(k) + "$$, " +
          v._4 + "," +
          v._1 + "," +
          v._2 + "," +
          v._3 + "" +
          ");"))
      }
      session.execute(batch)

      return true
    } finally {}
    false
  }

  def StoreSpeedingDistanceForDrivers(map : scala.collection.mutable.Map[String,(Float, Int, Int, Float)]): Boolean = {
    try {
      session.execute("Truncate speedingbydistancebydriver;")

      val batch = new BatchStatement()

      for((k, v) <- map){
        batch.add(
          new SimpleStatement(
            "INSERT INTO ctrack.speedingbydistancebydriver (" +
              "partkey, " +
              "driverid, " +
              "significance, " +
              "speedingpercent, " +
              "speedingmeters, " +
              "totaldrivenmeters" +
              ") VALUES (" +
              "1, " +
              "$$" + StringEscapeUtils.escapeJava(k) + "$$, " +
              v._4 + "," +
              v._1 + "," +
              v._2 + "," +
              v._3 + "" +
              ");"))
      }
      session.execute(batch)

      return true
    } finally {}
    false
  }

  def StoreHighSpeedEvents(map: ListBuffer[(String, Int, String, Float, DateTime)]): Boolean ={
    try {
      session.execute("Truncate highSpeedDrivers;")

      val batch = new BatchStatement()

      map.foreach( x => {
        batch.add(
          new SimpleStatement(
            "INSERT INTO ctrack.highSpeedDrivers (" +
              "partkey, " +
              "driverid, " +
              "mobilespeed, " +
              "street, " +
              "streetmaxspeed, " +
              "percentageover, " +
              "eventtime" +
              ") VALUES (" +
              "1, " +
              "$$" + StringEscapeUtils.escapeJava(x._1) + "$$, " +
              x._2 + "," +
              "$$" + StringEscapeUtils.escapeJava(x._3) + "$$, " +
              x._4 + "," +
              (((x._2/x._4)*100) - 100) + ", " +
              "$$" + StringEscapeUtils.escapeJava(x._5.toString) + "$$" +
              ");"))
      })

      session.execute(batch)

      System.out.println("Completed high speed speeding")

      return true
    } finally {}
      return false
  }

  def StoreEventsForStreets(map:
      scala.collection.mutable.Map[String,
    ((Float, Int, Float),
      (Float, Int, Float),
      (Float, Int, Float), Int)]){
    try {
      session.execute("Truncate harshEventsbyStreet;")
      var batches =  new ListBuffer[BatchStatement]
      var batch = new BatchStatement()

      for((k, v) <- map){
        if(batch.size() == 65535){
          batches += batch
          batch = new BatchStatement()
        }

        batch.add(
          new SimpleStatement(
            "INSERT INTO ctrack.harshEventsbyStreet (" +
              "partkey, " +
              "address, " +
              "totalmovements, " +
              "accpercent, " +
              "accevents, " +
              "accsignificance, " +
              "brkpercent, " +
              "brkevents, " +
              "brksignificance, " +
              "crnpercent, " +
              "crnevents, " +
              "crnsignificance " +
              ") VALUES (" +
              "1, " +
              "$$" + StringEscapeUtils.escapeJava(k) + "$$, " +
               v._4 + "," +
               v._1._1 + "," +
               v._1._2 + "," +
               v._1._3 + "," +
               v._2._1 + "," +
               v._2._2 + "," +
               v._2._3 + "," +
               v._3._1 + "," +
               v._3._2 + "," +
               v._3._3 +
               ");"))
      }

      batches += batch

      batches.foreach(x => session.executeAsync(x))

      System.out.println("Completed events By Street")

      return true
    } finally {}
    return false
  }
}
