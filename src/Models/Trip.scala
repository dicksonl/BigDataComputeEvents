package Models

import java.sql.Timestamp
import scala.collection.mutable.ListBuffer

class Trip (
    var VehicleNodeId : Int,
    var TripStart_UTC : Timestamp,
    var TripEnd_UTC : Timestamp,
    var TripStart_LT : Timestamp,
    var TripEnd_LT : Timestamp,
    var TripOdo : Float,
    var DrivingTime: Int,
    var SpeedLimit : Float,
    var RoadSpeedLimit : Float,
    var OverSpeedCount : Int,
    var MaxSpeed : Float,
    var OverSpeedDuration : Int,
    var HarshAccelerateCount : Int,
    var HarshCornerCount : Int,
    var HarshBrakeCount : Int,
    var HarshBumpCount : Int,
    var IncidentCount : Int,
    var ExcessIdlingCount : Int,
    var Odo : Float,
    var CitiesTraversed : String = "",
    var PostCodeTraversed : String = "",
    var Movements: ListBuffer[Movement] = new ListBuffer[Movement]
)
