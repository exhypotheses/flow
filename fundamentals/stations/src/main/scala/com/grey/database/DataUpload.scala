package com.grey.database

import com.grey.libraries.mysql.LoadData

import scala.util.Try


/**
  * Upload
  */
class DataUpload() {

  /**
    *
    * @param tableVariables: A Map of variables for creating & updating tables
    */
  def dataUpload(tableVariables: Map[String, String]): Try[Boolean] = {

    // Uploading
    val loadData = new LoadData()
    val upload: Try[Boolean] = loadData.loadData(databaseString = "mysql.flow",
      tableVariables = tableVariables)

    // Hence
    if (upload.isSuccess) {
      upload
    } else {
      // Superfluous
      sys.error(upload.failed.get.getMessage)
    }

  }

}
