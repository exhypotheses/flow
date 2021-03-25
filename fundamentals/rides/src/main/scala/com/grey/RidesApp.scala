package com.grey

import com.grey.environment.{DataDirectories, LocalSettings}
import com.grey.source.{InterfaceTimeSeries, InterfaceVariables}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try

object RidesApp {

  private val localSettings = new LocalSettings()

  def main(args: Array[String]): Unit = {

    // Minimising log information output
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("aka").setLevel(Level.OFF)


    // Spark Session
    val spark = SparkSession.builder().appName("stations")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.warehouse.dir", localSettings.warehouseDirectory)
      .getOrCreate()


    // Prepare local directories
    val dataDirectories = new DataDirectories()
    val directories: ParSeq[Try[Boolean]] = List(localSettings.dataDirectory, localSettings.warehouseDirectory)
      .par.map( directory => dataDirectories.localDirectoryReset(directory) )


    // Dates
    val interfaceVariables = new InterfaceVariables(spark = spark)
    val (listOfDates, filterDate): (List[DateTime], DateTime) = new InterfaceTimeSeries(spark = spark)
      .interfaceTimeSeries(interfaceVariables = interfaceVariables)


    // Proceed
    if (directories.head.isSuccess) {
      new DataSteps(spark = spark)
        .dataSteps(listOfDates = listOfDates, filterDate = filterDate)
    } else {
      sys.error(directories.head.failed.get.getMessage)
    }


  }


}
