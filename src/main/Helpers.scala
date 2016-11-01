package main

import com.datastax.spark.connector.CassandraRow

object Helpers {
    def getRowsByHourRange(row : CassandraRow, from : Int, to: Int): Boolean ={
      val ddate = row.getDateTime("ddate")
      val hourOfDate = ddate.getHourOfDay

      if(hourOfDate > from && hourOfDate < to) {
        return true
      }
        false
    }


}
