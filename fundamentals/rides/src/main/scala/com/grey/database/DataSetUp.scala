package com.grey.database

import java.io.File
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import com.grey.environment.LocalSettings

import scala.collection.parallel.mutable.ParArray
import scala.util.Try
import scala.util.control.Exception

class DataSetUp {
  
  private val localSettings = new LocalSettings()
  
  def dataSetUp(fileObjects: Array[File]): Try[ParArray[Path]] = {

    // Prepare data files for database upload
    val setUp: Try[ParArray[Path]] = Exception.allCatch.withTry(
      
      fileObjects.par.map{fileObject =>        
        Files.move(
          Paths.get(fileObject.toString),
          Paths.get(localSettings.root, fileObject.getParentFile.getName + fileObject.getName),
          StandardCopyOption.REPLACE_EXISTING
        )
      }
      
    )
    
    // Hence
    if (setUp.isSuccess){
      setUp
    } else {
      sys.error(setUp.failed.get.getMessage)
    }
        
  }

}
