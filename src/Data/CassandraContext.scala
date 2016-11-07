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
}
