package ca.mcit.bigdata.project4

object Main extends Config with App {

  val queryTrips = stmt.execute("DROP TABLE IF EXISTS bdss2001_vish.ext_trips")
  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS bdss2001_vish.ext_trips (
      |trip_id int,
      |service_id STRING,
      |route_id STRING,
      |trip_headsign STRING,
      |wheelchair_accessible INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/bdss2001/vish1/project4/trips/'
      |TBLPROPERTIES (
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = ""
      |)""".stripMargin)

  println("table exists")

  val queryCalDates = stmt.execute("DROP TABLE IF EXISTS bdss2001_vish.ext_calendar_dates")
  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS bdss2001_vish.ext_calendar_dates (
      |service_id STRING,
      |new_date STRING,
      |exception_type INT
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/bdss2001/vish1/project4/calendar_dates/'
      |TBLPROPERTIES (
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = ""
      |)""".stripMargin)

  println("table exists")

  val queryRoutes = stmt.execute("DROP TABLE IF EXISTS bdss2001_vish.ext_routes")
  stmt.executeUpdate(
    """CREATE EXTERNAL TABLE IF NOT EXISTS bdss2001_vish.ext_routes (
      |route_id INT,
      |route_long_name STRING,
      |route_color STRING
      |)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS TEXTFILE
      |LOCATION '/user/bdss2001/vish1/project4/routes/'
      |TBLPROPERTIES (
      |    "skip.header.line.count" = "1",
      |    "serialization.null.format" = ""
      |)""".stripMargin)

  println("table exists")

  stmt execute ("""SET hive.mapred.mode = nonstrict""")
  stmt execute ("""SET hive.exec.dynamic.partition.mode = nonstrict""")
  stmt execute ("""SET hive.auto.convert.join=false;""")
  val queryEnrichedTrip = stmt.execute("DROP TABLE IF EXISTS bdss2001_vish.enriched_trip")
  stmt.executeUpdate(
    """CREATE TABLE IF NOT EXISTS bdss2001_vish.enriched_trip (
      |trip_id int,
      |service_id STRING,
      |route_id STRING,
      |trip_headsign STRING,
      |new_date STRING,
      |exception_type INT,
      |route_long_name STRING,
      |route_color STRING
      |)
      |PARTITIONED BY (wheelchair_accessible INT)
      |ROW FORMAT DELIMITED
      |FIELDS TERMINATED BY ','
      |STORED AS PARQUET
      |""".stripMargin)

  println("table exists")

  stmt.executeUpdate(
    """INSERT INTO bdss2001_vish.enriched_trip PARTITION(wheelchair_accessible)
      |SELECT
      |A.trip_id,
      |A.service_id,
      |A.route_id,
      |A.trip_headsign,
      |A.wheelchair_accessible,
      |B.new_date,
      |B.exception_type,
      |C.route_long_name,
      |C.route_color
      |FROM bdss2001_vish.ext_trips A
      |LEFT JOIN bdss2001_vish.ext_calendar_dates B
      |ON A.service_id = B.service_id
      |LEFT JOIN bdss2001_vish.ext_routes C
      |ON A.route_id = C.route_id""".stripMargin)

  stmt.close()
  connection.close()
}
