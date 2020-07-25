package com.grey.trips.hive

import com.grey.trips.environment.{ConfigParameters, LocalSettings}
import com.grey.libraries.hive.HiveBaseCaseClass

class HiveBaseProperties {

  private val localSettings = new LocalSettings()
  private val configParameters = new ConfigParameters()

  def hiveBaseProperties: HiveBaseCaseClass#HBCC = {


    // Database
    val databaseName = "flow"
    val commentDatabase = "Data particular to flow"
    val databaseProperties = "'creator' = 'grey hypotheses'"
    val databaseLocation = localSettings.databaseDirectory


    // Table: Miscellaneous
    val tableName = "trips"
    val commentTable = "Baseline trips data"
    val rowFormat = "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n'"
    val storedAs = "TEXTFILE"
    val tableLocation = localSettings.tableDirectory


    // This feature fails, hence .option("header", "false") is used to write files
    // val tblProperties = "TBLPROPERTIES ('serialization.null.format'='', 'skip.header.line.count'='1')"
    val tblProperties = ""


    // Table: Partitioning
    val partitionVariables: Map[String, String] = configParameters.partitionVariables


    // Case -> CREATE TABLE
    val partitionString: String = partitionVariables.toList.map { case (key: String, value: String) =>
      key + " " + value
    }.mkString(", ")


    // Clustering, Sorting, Bucketing
    val bucketVariables: Map[String, String] = Map("clusterBy" -> "start_station_id",
      "sortBy" -> "start_station_id, started_at", "numberOfBuckets" -> configParameters.nParallelism.toString)

    val bucketClause: String = s"CLUSTERED BY (${bucketVariables("clusterBy")}) " +
      s"SORTED BY (${bucketVariables("sortBy")}) INTO ${bucketVariables("numberOfBuckets")} BUCKETS"


    // The Hive Properties
    new HiveBaseCaseClass().HBCC(databaseName, commentDatabase, databaseProperties, databaseLocation, tableName,
      commentTable, tableLocation, rowFormat, storedAs, tblProperties, partitionString, bucketClause)


  }


}
