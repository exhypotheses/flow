package com.grey.database

class TableVariables {


  def tableVariables(): Map[String, String] = {


    // Table name
    val tableName = "flow.stations"


    // Create statement
    val stringCreateTable: String =
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName (
         |    station_id VARCHAR(15) NOT NULL PRIMARY KEY,
         |    capacity SMALLINT DEFAULT NULL,
         |    latitude DECIMAL(24, 21) NOT NULL,
         |    longitude DECIMAL(24, 21) NOT NULL,
         |    name VARCHAR(255) NOT NULL,
         |    address VARCHAR(1023) DEFAULT NULL
         |);
       """.stripMargin


    // Load
    // https://dev.mysql.com/doc/refman/8.0/en/load-data.html
    // LOCAL, FILESTRING, REPLACE | IGNORE
    val uploadString =
    raw"""
       | LOAD DATA %s INFILE '%s' %s INTO TABLE $tableName FIELDS TERMINATED BY ','  OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n';
       """.stripMargin


    // Hence
    Map("stringCreateTable" -> stringCreateTable.toString, "uploadString" -> uploadString.toString,
      "tableName" -> tableName.toString)

  }


}
