package com.grey.trips.environment

import java.nio.file.Paths

class LocalSettings {

  // The operating system
  val operatingSystem: String = System.getProperty("os.name").toUpperCase
  val operatingSystemWindows: Boolean = operatingSystem.startsWith("WINDOWS")


  // Local characteristics
  val localDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")
  val localWarehouse: String = s"$localDirectory${localSeparator}warehouse$localSeparator"


  // Hive Database
  val base: String = "database"
  val database: String = "flow"


  // These variables point to the same directory but their conventions differ
  val table: String = "trips"
  val databaseDirectory: String = "file:///Q:" + s"/$base/$database"
  val tableDirectory: String = databaseDirectory + s"/$table/"

  val warehouseDirectory: String = "Q:" + localSeparator + base +
    localSeparator + database + localSeparator + table + localSeparator


  // Local data directories
  val resourcesDirectory: String = s"$localDirectory${localSeparator}src" +
    s"${localSeparator}main${localSeparator}resources$localSeparator"
  val dataDirectory: String = s"$localDirectory${localSeparator}data$localSeparator"


}
