package Models

import java.sql.Timestamp

class Movement (
  var VehicleNodeId : Int,
  var MobileId : Option[String],
  var TripStart_UTC : Option[Timestamp],
  var Ddate : Timestamp,
  var Ddate_LT : Timestamp,
  var DriverId : Option[String],
  var MobileStatus : Option[String],
  var Status1 : Int,
  var MobileSpeed : Int,
  var Place : Option[String],
  var Longitude : Float,
  var Latitude : Float,
  var Heading : Int,
  var StreetMaxSpeed : Float,
  var Street : Option[String],
  var Suburb : Option[String],
  var City : Option[String],
  var ZipCode : Option[String],
  var Country : Option[String],
  var LocationName : Option[String],
  var LocationDistance : Float,
  var AreaGroup  : Int
)