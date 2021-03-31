package com.grey.database


class TableVariables {

  /**
    * Ref. https://dev.mysql.com/doc/refman/8.0/en/load-data.html
    *
    * @param isLocal: true | false -> is the file locally located (true) or hosted within a server (false)
    * @param infile: The path to the file location; it includes the file name & extension
    * @param duplicates: Duplicates: REPLACE | IGNORE
    * @return
    */
  def tableVariables(isLocal: Boolean = false,
                     infile: String = "", duplicates: String = ""): Map[String, String] = {


    // Table name
    val tableName = "rides"


    // Create statement
    val stringCreateTable: String =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    started_at TIMESTAMP NOT NULL,
         |    start_station_id VARCHAR(15) NOT NULL REFERENCES stations (station_id) ON UPDATE CASCADE ON DELETE RESTRICT,
         |    ended_at TIMESTAMP DEFAULT NULL,
         |    end_station_id VARCHAR(15) DEFAULT NULL REFERENCES stations (station_id) ON UPDATE CASCADE ON DELETE RESTRICT,
         |    duration BIGINT DEFAULT NULL,
         |    start_date DATE NOT NULL,
         |    start_date_epoch BIGINT NOT NULL
         |    ) PARTITION BY RANGE (start_date);
         |
         |    COMMENT ON COLUMN $tableName.started_at IS 'The start date and time of the ride';
         |    COMMENT ON COLUMN $tableName.start_station_id IS 'The unique identifier of the start station' ;
         |    COMMENT ON COLUMN $tableName.ended_at IS 'The end date and time of the ride';
         |    COMMENT ON COLUMN $tableName.end_station_id IS 'The unique identifier of the end station';
         |    COMMENT ON COLUMN $tableName.duration IS 'The duration of the ride in seconds';
         |    COMMENT ON COLUMN $tableName.start_date IS 'The date the ride started';
         |    COMMENT ON COLUMN $tableName.start_date_epoch IS 'UNIX Epoch from 1 January 1970 00 00 00 UTC';
       """.stripMargin


    // Statement
    val uploadString =
      s"""
         |COPY $tableName FROM '$infile' WITH
         |CSV
         |DELIMITER ','
         |HEADER
         |QUOTE '"'
         |ENCODING 'UTF8'
       """.stripMargin
    println(uploadString.toString)


    // Hence
    Map("stringCreateTable" -> stringCreateTable.toString, "uploadString" -> uploadString.toString,
      "tableName" -> tableName.toString)

  }


}
