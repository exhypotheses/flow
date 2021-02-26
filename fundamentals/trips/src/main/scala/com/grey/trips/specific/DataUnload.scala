package com.grey.trips.specific

import java.io.File
import java.net.URL

import com.grey.trips.environment.DataDirectories
import com.grey.trips.net.IsExistURL
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try
import scala.util.control.Exception

class DataUnload(spark: SparkSession) {

  private val interfaceVariables = new InterfaceVariables(spark = spark)
  private val isExistURL = new IsExistURL()
  private val dataDirectories = new DataDirectories()


  def dataUnload(dateTime: DateTime, directoryName: String, fileString: String): Try[String] = {

    // The application programming interface's URL for a data set of interest
    val url: String = interfaceVariables.api.format(dateTime.toString(interfaceVariables.dateTimePattern))

    // Is the URL alive?
    val isURL: Try[Boolean]  = isExistURL.isExistURL(url)

    // If yes, unload the data set, otherwise ...
    val unload: Try[String] = if (isURL.isSuccess) {
      dataDirectories.localDirectoryCreate(directoryName = directoryName)
      Exception.allCatch.withTry(
        new URL(url) #> new File(fileString) !!
      )
    } else {
      sys.error(isURL.failed.get.getMessage)
    }

    // Finally
    if (unload.isSuccess) {
      println("Successfully unloaded " + url)
      unload
    } else {
      sys.error(unload.failed.get.getMessage)
    }

  }

}
