package com.grey.directories

class LocalSettings {

  // The operating system
  val operatingSystem: String = System.getProperty("os.name").toUpperCase
  val operatingSystemWindows: Boolean =  operatingSystem.startsWith("WINDOWS")


  // Local project directory, and directory separation convention
  val localDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")


  // Local data directories
  val warehouseDirectory: String = s"$localDirectory${localSeparator}spark-warehouse$localSeparator"
  val resourcesDirectory: String = s"$localDirectory${localSeparator}src" +
    s"${localSeparator}main${localSeparator}resources$localSeparator"
  val dataDirectory: String = s"$localDirectory${localSeparator}data$localSeparator"


}
