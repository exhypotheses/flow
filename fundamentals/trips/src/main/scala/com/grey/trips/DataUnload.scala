package com.grey.trips

import java.io.File
import java.net.URL

import com.grey.net.IsExistURL
import org.joda.time.DateTime

import scala.language.postfixOps
import scala.sys.process._
import scala.util.Try
import scala.util.control.Exception

class DataUnload {

  private val interfaceVariables = new InterfaceVariables()
  private val isExistURL = new IsExistURL()
  private val appDirectories = new AppDirectories()


  def dataUnload(dateTime: DateTime, directoryName: String, fileString: String): Try[String] = {

    // The application programming interface's URL for a data set of interest
    val url: String = interfaceVariables.api.format(dateTime.toString(interfaceVariables.dateTimePattern))

    // Is the URL alive?
    val isURL: Try[Boolean]  = isExistURL.isExistURL(url)

    // If yes, unload the data set, otherwise ...
    val unload: Try[String] = if (isURL.isSuccess) {
      appDirectories.localDirectoryCreate(directoryName = directoryName)
      Exception.allCatch.withTry(
        new URL(url) #> new File(fileString) !!
      )
    } else {
      sys.error(isURL.failed.get.getMessage)
    }

    // Finally
    if (unload.isSuccess) {
      println("Successfully downloaded " + url)
      unload
    } else {
      sys.error(unload.failed.get.getMessage)
    }

  }

}
