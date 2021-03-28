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
  def tableVariables(isLocal: Boolean = true,
                     infile: String = "", duplicates: String = ""): Map[String, String] = {


    // Table name
    val tableName = "flow.rides"


    // Create statement
    val stringCreateTable: String =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    rides_id INTEGER NOT NULL AUTO_INCREMENT,
         |    started_at TIMESTAMP NOT NULL COMMENT 'The start date and time of the ride',
         |    start_station_id VARCHAR(15) NOT NULL COMMENT 'The unique identifier of the start station',
         |    ended_at TIMESTAMP DEFAULT NULL COMMENT 'The end date and time of the ride',
         |    end_station_id VARCHAR(15) DEFAULT NULL COMMENT 'The unique identifier of the end station',
         |    duration BIGINT UNSIGNED DEFAULT NULL COMMENT 'The duration of the ride in seconds',
         |    start_date DATE NOT NULL COMMENT 'The date the ride started',
         |    start_date_epoch BIGINT UNSIGNED NOT NULL COMMENT 'UNIX Epoch from 1 January 1970 00 00 00 UTC',
         |    PRIMARY KEY (rides_id, start_date_epoch),
         |    FOREIGN KEY (start_station_id) REFERENCES stations (station_id) ON UPDATE CASCADE ON DELETE RESTRICT,
         |    FOREIGN KEY (end_station_id) REFERENCES stations (station_id) ON UPDATE CASCADE ON DELETE RESTRICT
         |    );
       """.stripMargin


    // Statement
    val location: String = if (isLocal) "LOCAL" else ""

    val uploadString =
      raw"""
           | LOAD DATA $location INFILE '$infile' ${duplicates.toUpperCase} INTO TABLE $tableName FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;
       """.stripMargin


    // Hence
    Map("stringCreateTable" -> stringCreateTable.toString, "uploadString" -> uploadString.toString,
      "tableName" -> tableName.toString)

  }



}

/**
  Beware, foreign keys are not yet supported in conjunction with partitioning, hence the table
  is being created without partitioning.

  val stringCreateTable: String =
    s"""
      |CREATE TABLE IF NOT EXISTS $tableName (
      |   rides_id INTEGER NOT NULL AUTO_INCREMENT,
      |   started_at TIMESTAMP NOT NULL COMMENT 'The start date and time of the ride',
      |   start_station_id VARCHAR(15) NOT NULL COMMENT 'The unique identifier of the start station',
      |   ended_at TIMESTAMP DEFAULT NULL COMMENT 'The end date and time of the ride',
      |   end_station_id VARCHAR(15) DEFAULT NULL COMMENT 'The unique identifier of the end station',
      |   duration BIGINT UNSIGNED DEFAULT NULL COMMENT 'The duration of the ride in seconds',
      |   start_date DATE NOT NULL COMMENT 'The date the ride started',
      |   start_date_epoch BIGINT UNSIGNED NOT NULL COMMENT 'UNIX Epoch from 1 January 1970 00 00 00 UTC',
      |   PRIMARY KEY (rides_id, start_date_epoch))
      |   PARTITION BY HASH(start_date_epoch)
      |   PARTITIONS 8;
     """.stripMargin
  */
