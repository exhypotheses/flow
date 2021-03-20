package com.grey.source

import java.io.{File, FileFilter}

import com.grey.environment.LocalSettings
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.util.control.Exception


/**
  *
  * @param spark: An instance of SparkSession
  */
class DataTransform(spark: SparkSession) {
  
  private val localSettings = new LocalSettings()


  /**
    * explode: is for data Array decomposition, whilst
    * variable.*: is for Fields decomposition
    *
    * @param unloadString: The string of the file to be read; it includes the path to the file and the file extension
    * @return
    */
  def dataTransform(unloadString: String): Try[String] = {

    /**
      * Import implicits for
      * encoding (https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-sql-Encoder.html)
      * implicit conversions, e.g., converting a RDD to a DataFrames.
      * access to the "$" notation.
      */
    import spark.implicits._
    
    
    // Read
    val data  = Exception.allCatch.withTry(
      spark.read.json(unloadString)
    )
    if (data.isFailure) {
      sys.error(data.failed.get.getMessage)
    }


    // Target directory after transformation steps
    val directory = localSettings.warehouseDirectory + "stations"
    

    // Data structuring: decompose horizontally
    var nests: DataFrame = data.get.select($"data.*", $"last_updated", $"ttl")
    
    
    // ... decompose vertically, i.e., decompose array
    nests = nests.select(explode($"stations").as("station"), $"last_updated", $"ttl")
    
    
    // ... decompose horizontally, and rename
    val stations = nests.select($"station.*")
      .select($"station_id", $"capacity", $"lat".as("latitude"), $"lon".as("longitude"),
        $"name", $"address")
    stations.show(5)


    // Save
    stations.coalesce(1).write.option("header", "true").option("encoding", "UTF-8")
      .csv(directory)


    /**
      * The data has been transformed, and subsequently saved in a
      * single file.  What is the file's name?
      */

    // Directory object
    val directoryObject = new File(directory)

    // Filter
    val fileFilter: FileFilter = new WildcardFileFilter("*.csv")

    // The file string
    val getFileString = Exception.allCatch.withTry(
      directoryObject.listFiles(fileFilter).head.toString
    )

    if (getFileString.isSuccess) {
      getFileString
    } else {
      sys.error(getFileString.failed.get.getMessage)
    }


  }

}

/**
  * // Determine  extraneous files
  * val listOfArrays: List\[Array\[File\]\] = List("*SUCCESS", "*.crc").map{ string =>
  *   val fileFilter: FileFilter = new WildcardFileFilter(string)
  *         directoryObject.listFiles(fileFilter)
  * }
  *
  * // Eliminate the extraneous files
  * val eliminate: Try[Unit] = Exception.allCatch.withTry(
  *       listOfArrays.reduce( _ union _).par.foreach(x => x.delete())
  * )
  *
  * // Hence, the directory in question consists of valid data files only
  * if (eliminate.isSuccess) {
  * val fileFilter: FileFilter = new WildcardFileFilter("*.csv")
  *       directoryObject.listFiles(fileFilter)
  * } else {
  *       sys.error(eliminate.failed.get.getMessage)
  * }
  */