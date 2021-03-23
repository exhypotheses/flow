package com.grey.source

import java.io.File
import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.Try
import scala.util.control.Exception
import com.grey.environment.LocalSettings


/**
  * The earliest data month is "2018/12"
  * https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  * private val monthLimitDateTime: DateTime =
  *     DateTimeFormat.forPattern(dateTimePattern).parseDateTime(monthLimitString)
  *
  * @param spark: Instance
  */
class InterfaceVariables(spark: SparkSession) {


  /**
    * Path
    */
  private val localSettings = new LocalSettings()
  private val path: String = Paths.get(localSettings.resourcesDirectory, "cycles.conf").toString


  /**
    * Read the configuration file
    */
  private val config: Try[Config] = Exception.allCatch.withTry(
    ConfigFactory.parseFile(new File(path)).getConfig("variables")
  )
  if (config.isFailure) {
    sys.error(config.failed.get.getMessage)
  }


  /**
    * Query formula
    */
  val variable: (String, String) => String = (group: String, variable: String) => {
    val text = Exception.allCatch.withTry(
      config.get.getConfig(group).getString(variable)
    )
    if (text.isFailure) {
      sys.error(text.failed.get.getMessage)
    } else {
      text.get
    }
  }


  /**
    * Set the variables
    */
  val step: Int = variable("times", "step").toInt
  val stepType: String = variable("times", "stepType")
  val dateTimePattern: String = variable("url", "dateTimePattern")
  val api: String = variable("url", "api")

}
