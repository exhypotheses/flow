package com.grey.pre

import java.io.{File, FileFilter}
import java.nio.file.Paths

import com.grey.environment.LocalSettings
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql._

import scala.util.Try
import scala.util.control.Exception


/**
  *
  */
class DataWrite() {

  private val localSettings = new LocalSettings()

  /**
    *
    * @param data: The data set that will be saved
    * @param name: The name of the directory leaf into wherein the data is saved
    * @return
    */
  def dataWrite(data: DataFrame, name: String): Array[File] = {

    
    // Directory object
    val directory = localSettings.warehouseDirectory + name
    val directoryObject = new File(directory)
    
    
    // Beware of time stamps, e.g., "yyyy-MM-dd HH:mm:ss.SSSXXXZ"
    val stream: Try[Unit] = Exception.allCatch.withTry(
      data.write
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
