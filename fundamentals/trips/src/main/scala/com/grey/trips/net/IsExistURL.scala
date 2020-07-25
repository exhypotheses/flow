package com.grey.trips.net

import java.net.{HttpURLConnection, URL}

import scala.util.Try
import scala.util.control.Exception

class IsExistURL {

  def isExistURL(urlString: String): Try[Boolean] = {


    // Do not follow re-directs
    HttpURLConnection.setFollowRedirects(false)


    // Determine whether the URL exists
    val request: Try[Boolean] = Exception.allCatch.withTry({

      val httpURLConnection: HttpURLConnection = new URL(urlString).openConnection().asInstanceOf[HttpURLConnection]
      httpURLConnection.setInstanceFollowRedirects(false)
      httpURLConnection.setRequestMethod("HEAD")
      httpURLConnection.setConnectTimeout(5000) // milliseconds
      httpURLConnection.setReadTimeout(5000) // milliseconds

      httpURLConnection.getResponseCode == HttpURLConnection.HTTP_OK

    })


    // Hence
    if (request.isSuccess) {
      request
    } else {
      sys.error(s"Error: ($urlString)" + request.failed.get.getMessage)
    }


  }

}
