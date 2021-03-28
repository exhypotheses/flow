package com.grey.pre

import java.io.{File, FileFilter}

import com.grey.environment.LocalSettings
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.spark.sql.{Dataset, Row}

import scala.util.Try
import scala.util.control.Exception

class DataWrite {

  private val localSettings = new LocalSettings()

  def dataWrite(data: Dataset[Row]): Array[File] = {


    // Directory object
    val directory = localSettings.warehouseDirectory
    val directoryObject = new File(directory)


    // Save
    val stream: Try[Unit] = Exception.allCatch.withTry(
      data.coalesce(1).write.option("header", "true").option("encoding", "UTF-8")
        .csv(directory)
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
