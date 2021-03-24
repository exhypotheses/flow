package com.grey.source

import java.nio.file.Paths

import com.grey.environment.LocalSettings
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
  def dataWrite(minimal: DataFrame, name: String): Try[Unit] = {

    /**
      * This import is required for (a) the $-notation, (b) implicit conversions such as converting a RDD
      * to a DataFrame, (c) encoders for [most] types, which are also automatically provided by
      * via spark.implicits._
      *
      * import spark.implicits._
      *
      */


    // Beware of time stamps, e.g., "yyyy-MM-dd HH:mm:ss.SSSXXXZ"
    val fieldsOfInterest = List()
    val stream: Try[Unit] = Exception.allCatch.withTry(
      minimal.selectExpr(fieldsOfInterest: _*).write
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("timestampFormat", localSettings.projectTimeStamp)
        .csv(Paths.get(localSettings.warehouseDirectory + name, "").toString)
    )


    // Hence
    if (stream.isSuccess) {
      stream
    } else {
      sys.error(stream.failed.get.getMessage)
    }


  }

}
