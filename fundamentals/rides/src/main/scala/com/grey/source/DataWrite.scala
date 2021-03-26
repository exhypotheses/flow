package com.grey.source

import java.io.{File, FileFilter}
import java.nio.file.Paths

import com.grey.environment.LocalSettings
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql._

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataWrite(spark: SparkSession) {

  private val localSettings = new LocalSettings()


  /**
    *
    * @param minimal: The data set that will be saved
    * @param name: The name of the directory leaf into wherein the data is saved
    * @return
    */
  def dataWrite(minimal: DataFrame, name: String): Array[File] = {

    /**
      * This import is required for (a) the $-notation, (b) implicit conversions such as converting a RDD
      * to a DataFrame, (c) encoders for [most] types, which are also automatically provided by
      * via spark.implicits._
      *
      * import spark.implicits._
      *
      */
        

    // Directory object
    val directory = localSettings.warehouseDirectory + name
    val directoryObject = new File(directory)
    
    
    // Beware of time stamps, e.g., "yyyy-MM-dd HH:mm:ss.SSSXXXZ"
    val fieldsOfInterest = List("started_at", "start_station_id", "ended_at", "end_station_id",
      "duration", "start_date", "start_date_epoch")
    val stream: Try[Unit] = Exception.allCatch.withTry(
      minimal.selectExpr(fieldsOfInterest: _*).write
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("timestampFormat", localSettings.projectTimeStamp)
        .csv(Paths.get(directory, "").toString)
    )


    // Determine extraneous files ... extraneous files array (EFA)
    val listOfEFA = if (stream.isSuccess){
      List("*SUCCESS", "*.crc").map{ string =>
        val fileFilter: FileFilter = new WildcardFileFilter(string)
        directoryObject.listFiles(fileFilter)
      }
    } else {
      sys.error(stream.failed.get.getMessage)
    }


    // Eliminate the extraneous files
    val eliminate: Try[Unit] = Exception.allCatch.withTry(
      listOfEFA.reduce( _ union _).par.foreach(x => x.delete())
    )


    // Hence, the directory in question consists of valid data files only
    if (eliminate.isSuccess) {
      val fileFilter: FileFilter = new WildcardFileFilter("*.csv")
      directoryObject.listFiles(fileFilter)
    } else {
      sys.error(eliminate.failed.get.getMessage)
    }


  }


}
