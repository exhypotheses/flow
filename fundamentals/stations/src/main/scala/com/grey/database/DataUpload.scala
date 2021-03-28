package com.grey.database

import com.grey.environment.{DataDirectories, LocalSettings}
import com.grey.libraries.mysql.LoadData

import scala.util.Try


/**
  * Upload
  */
class DataUpload() {

  private val localSettings = new LocalSettings()

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

      new DataDirectories()
        .localDirectoryDelete(directoryName = localSettings.warehouseDirectory)

      upload
    } else {
      // Superfluous
      sys.error(upload.failed.get.getMessage)
    }

  }

}
