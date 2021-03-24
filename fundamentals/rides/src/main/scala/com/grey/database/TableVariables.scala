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
                     infile: String = null, duplicates: String = null): Map[String, String] = {


    // Table name
    val tableName = "flow.rides"


    // Create statement
    val stringCreateTable: String =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    rides_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
         |    started_at TIMESTAMP NOT NULL,
         |    start_station_id VARCHAR(15) NOT NULL,
         |    ended_at TIMESTAMP DEFAULT NULL,
         |    end_station_id VARCHAR(15) DEFAULT NULL,
         |    duration BIGINT UNSIGNED DEFAULT NULL,
         |    start_date DATE NOT NULL
         |)   PARTITION BY KEY(start_date, start_station_id)
         |    PARTITIONS 8;
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
