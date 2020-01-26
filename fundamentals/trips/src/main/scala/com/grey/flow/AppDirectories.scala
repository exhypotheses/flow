package com.grey.flow

import java.io.File

import org.apache.commons.io.FileUtils

import scala.util.Try
import scala.util.control.Exception

class AppDirectories {
  
  def localDirectories(directoryName: String): Try[Boolean] = {

    val directoryObject = new File(directoryName)

    val f = Exception.allCatch.withTry(
      if (directoryObject.exists()) {
        FileUtils.deleteDirectory(directoryObject)
      } else {

      }
    )

    if (f.isSuccess) {
      Exception.allCatch.withTry(
        directoryObject.mkdir()
      )
    } else {
      sys.error(f.failed.get.getMessage)
    }

  }


  def localDirectoryCreate(directoryName: String): Unit = {

    val directoryObject = new File(directoryName)

    if (!directoryObject.exists()) {
      directoryObject.mkdir()
    }

  }


}
