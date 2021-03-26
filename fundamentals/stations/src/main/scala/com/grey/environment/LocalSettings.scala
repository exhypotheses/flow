package com.grey.environment

import java.nio.file.{Path, Paths}

class LocalSettings {


  // The operating system
  val operatingSystem: String = System.getProperty("os.name").toUpperCase
  val operatingSystemWindows: Boolean = operatingSystem.startsWith("WINDOWS")


  // Local characteristics
  val localDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")


  // The local warehouse directory
  val keys: Map[String, String] = new com.grey.libraries.DataDatabase()
    .dataDatabase(databaseName = "mysql.flow")
  val root: Path = Paths.get(keys("operations"))

  val warehouseDirectory: String = Paths
    .get(root.toAbsolutePath.toString, "warehouse").toString + localSeparator


  // Other local directories
  val resourcesDirectory: String = Paths.get(localDirectory, "src", "main", "resources").toString + localSeparator
  val dataDirectory: String = Paths.get(localDirectory, "data").toString + localSeparator


}
