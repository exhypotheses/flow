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
  def tableVariables(isLocal: Boolean = false, infile: String = "", duplicates: String = "replace"): Map[String, String] = {


    // Table name
    val tableName = "flow.stations"


    // Create statement
    val stringCreateTable: String =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    station_id VARCHAR(15) NOT NULL PRIMARY KEY,
         |    capacity SMALLINT DEFAULT NULL,
         |    latitude NUMERIC(24, 21) NOT NULL,
         |    longitude NUMERIC(24, 21) NOT NULL,
         |    name VARCHAR(255) NOT NULL,
         |    address VARCHAR(1023) DEFAULT NULL
         |);
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
