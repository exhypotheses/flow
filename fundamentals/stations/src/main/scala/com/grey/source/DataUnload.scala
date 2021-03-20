package com.grey.source

import java.io.File
import java.net.URL

import com.grey.net.IsExistURL

import scala.util.Try
import scala.util.control.Exception

import scala.language.postfixOps
import scala.sys.process._

class DataUnload {

  def dataUnload(urlString: String, unloadString: String): Try[String] = {

    // Valid URL?
    val isExistURL: Try[Boolean] = new IsExistURL().isExistURL(urlString = urlString)


    // Unload
    val unload: Try[String] = if (isExistURL.isSuccess) {
      Exception.allCatch.withTry(
        new URL(urlString) #> new File(unloadString) !!
      )
    } else {
      sys.error(isExistURL.failed.get.getMessage)
    }

    unload


  }

}
