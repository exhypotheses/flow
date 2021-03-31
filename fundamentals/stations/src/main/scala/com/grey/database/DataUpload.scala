package com.grey.database

import com.grey.environment.LocalSettings
import com.grey.libraries.postgresql.CopyData

import scala.util.Try


/**
  * Upload
  */
class DataUpload() {

  private val localSettings = new LocalSettings()

  /**
    *
    * @param tableVariables : A Map of variables for creating & updating tables
    */
  def dataUpload(tableVariables: Map[String, String]): Try[Boolean] = {

    // Uploading
    val copyData = new CopyData()
    val copy: Try[Boolean] = copyData.copyData(databaseString = localSettings.databaseString,
      tableVariables = tableVariables)


    // Hence
    if (copy.isSuccess) {
      // new DataDirectories()
      //   .localDirectoryDelete(directoryName = localSettings.warehouseDirectory)
      copy
    } else {
      // Superfluous
      sys.error(copy.failed.get.getMessage)
    }

  }

}
