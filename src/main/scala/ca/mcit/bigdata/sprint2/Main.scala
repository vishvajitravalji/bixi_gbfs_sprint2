package ca.mcit.bigdata.sprint2

import io.circe.Decoder.Result
import io.circe.{Decoder, HCursor, Json, parser}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import scala.io.Source

object Main extends Config with App {

  // Create directory
  val stagingPath = new Path("/user/bdss2001/vish1/external")
  if (fs.exists(stagingPath)) fs.delete(stagingPath, true)
  fs.mkdirs(stagingPath)

  //Take JSON
  val systemInformationJson = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/system_information.json").mkString
  val stationInformationJson = Source.fromURL("https://gbfs.velobixi.com/gbfs/en/station_information.json").getLines().mkString

  //Put JSON
  val systemCsv: FSDataOutputStream = fs.create(new Path("/user/bdss2001/vish1/external/system_information/system_information.csv"), true)
  val stationCsv: FSDataOutputStream = fs.create(new Path("/user/bdss2001/vish1/external/station_information/station_information.csv"), true)

  //Decode system info
  parser.decode[SystemInformation](systemInformationJson) match {
    case Right(value) => systemCsv.writeBytes(SystemInformation.toCsv(value))
    case Left(ex) => println(s"Problem is - $ex")
  }

  //Decode nested Json
  implicit val stations = new Decoder[List[Json]] {
    override def apply(c: HCursor): Result[List[Json]] = {
      c.downField("data").downField("stations").as[List[Json]]
    }
  }

  //Decode station info
  parser.decode[List[Json]](stationInformationJson) match {
    case Right(value) => value.map(value => {
      parser.decode[StationInformation](value.toString) match {
        case Right(value) => stationCsv.writeBytes(StationInformation.toCsv(value))
        case Left(ex) => println(s"Problem is :-> ${ex}")
      }
    })
  }

  //Create database
  stmt.execute("CREATE DATABASE IF NOT EXISTS bdss2001_vish")

  //Manage tables
  stmt.execute("DROP TABLE bdss2001_vish.ext_system_information")
  stmt.execute("DROP TABLE bdss2001_vish.ext_station_information")
  stmt.execute("DROP TABLE bdss2001_vish.enriched_station_information")

  //external table of system information
  stmt.executeUpdate(
    """create external table if not exists bdss2001_vish.ext_system_information(
      |system_id STRING,
      |language STRING,
      |name STRING,
      |short_name STRING,
      |operator STRING,
      |url STRING,
      |purchase_url STRING,
      |start_date STRING,
      |phone_number STRING,
      |email STRING,
      |timezone STRING,
      |license_url STRING
      |) ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/bdss2001/vish1/external/system_information/'
        """.stripMargin)

  //external table of station information
  stmt.executeUpdate(
    """create external table if not exists bdss2001_vish.ext_station_information(
      |station_id INT,
      |external_id STRING,
      |name STRING,
      |short_name STRING,
      |lat DOUBLE,
      |lon DOUBLE,
      |rental_methods STRING,
      |capacity INT,
      |electric_bike_surcharge_waiver BOOLEAN,
      |is_charging BOOLEAN,
      |eightd_has_key_dispenser BOOLEAN,
      |has_kiosk BOOLEAN
      |) ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET
      |LOCATION '/user/bdss2001/vish1/external/station_information/'
           """.stripMargin)

  //create table for enriched station information
  stmt.executeUpdate(
    """create table if not exists bdss2001_vish.enriched_station_information(
      |system_id STRING,
      |timezone STRING,
      |station_id INT,
      |name STRING,
      |short_name STRING,
      |lat DOUBLE,
      |lon DOUBLE,
      |capacity INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET
      |LOCATION '/user/bdss2001/vish1/external/enriched/'
           """.stripMargin)

  //insert data using cross join
  stmt.executeUpdate(
    """|SET hive.auto.convert.join=false;
       |INSERT INTO TABLE bdss2001_vish.enriched_station_information
       |SELECT
       |system.system_id,
       |system.timezone,
       |station.station_id,
       |station.name,
       |station.short_name,
       |station.lat,
       |station.lon,
       |station.capacity
       |FROM
       |bdss2001_vish.ext_system_information AS system
       |CROSS JOIN
       |bdss2001_vish.ext_station_information AS station
    """.stripMargin)

  stmt.close()
  connection.close()

}
