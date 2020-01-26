package com.grey.flow

import java.io.File

import org.apache.commons.io.FileUtils

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

class AppDirectories {
  
  def localDirectories(directoryName: String): Unit = {

    val directoryObject = new File(directoryName)
    val f = Future{
      if (directoryObject.exists()) {
        FileUtils.deleteDirectory(directoryObject)
      }
    }
    f.onComplete{
      case Success(_) =>
        directoryObject.mkdir()
      case Failure(exception) =>
        exception.printStackTrace()
        sys.exit()
    }

  }

}
