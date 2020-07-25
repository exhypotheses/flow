package com.grey.trips.functions

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.grey.trips.environment.DataDirectories

import scala.collection.parallel.mutable.ParArray
import scala.util.Try
import scala.util.control.Exception

class DataReset {

  private val dataDirectories = new DataDirectories()

  def dataReset(source: String, destination: String): ParArray[Try[Path]] = {


    // Ensure the destination directory is empty
    val directory: Try[Unit] = Exception.allCatch.withTry(
      dataDirectories.localDirectoryReset(destination)
    )


    // Get the array of files of interest
    val filesObject = new File(source)
    val arrayOfFiles: Array[File] = filesObject.listFiles.filter(_.getName.endsWith(".csv"))


    // Move files
    if (directory.isSuccess) {
      arrayOfFiles.par.map{file =>
        val relocate: Try[Path] = Exception.allCatch.withTry(Files.move(Paths.get(file.toString),
            Paths.get(destination + file.getName), StandardCopyOption.REPLACE_EXISTING))
        if (relocate.isFailure){
          sys.error(relocate.failed.get.getMessage)
        } else {
          relocate
        }
      }
    } else {
      sys.error(directory.failed.get.getMessage)
    }

  }

}
