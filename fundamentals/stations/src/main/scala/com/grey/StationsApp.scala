package com.grey

import java.io.File

import com.grey.environment.{DataDirectories, LocalSettings}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.parallel.immutable.ParSeq
import scala.util.Try


object StationsApp {

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

    val rootObject = new File(localSettings.root)
    rootObject.listFiles().par.foreach(_.delete())


    // Proceed
    if (directories.head.isSuccess) {
      new DataSteps(spark = spark).dataSteps()
    } else {
      sys.error(directories.head.failed.get.getMessage)
    }


  }


}
