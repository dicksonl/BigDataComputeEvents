package Data

import com.datastax.driver.core.{PoolingOptions, _}
import org.apache.commons.lang.StringEscapeUtils

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

  def StoreEventsForStreets(map:
      scala.collection.mutable.Map[String,
    ((Float, Int, Int, Float),
      (Float, Int, Float),
      (Float, Int, Float))]){
    try {
      session.execute("Truncate harshEventsbyStreet;")

      val batch = new BatchStatement()

      for((k, v) <- map){
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
               v._1._3 + "," +
               v._1._1 + "," +
               v._1._2 + "," +
               v._1._4 + "," +
               v._2._1 + "," +
               v._2._2 + "," +
               v._2._3 + "," +
               v._3._1 + "," +
               v._3._2 + "," +
               v._3._3 +
               ");"))
      }
      session.execute(batch)

      return true
    } finally {}
    false
  }
}
