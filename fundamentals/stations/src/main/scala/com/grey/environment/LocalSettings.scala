package com.grey.environment

import java.nio.file.Paths

class LocalSettings {

  // The operating system
  val operatingSystem: String = System.getProperty("os.name").toUpperCase
  val operatingSystemWindows: Boolean = operatingSystem.startsWith("WINDOWS")

  // Local characteristics
  val localDirectory: String = System.getProperty("user.dir")
  val localSeparator: String = System.getProperty("file.separator")

  // Local data directories
  val warehouseDirectory: String = Paths.get(localDirectory, "warehouse").toString + localSeparator
  val resourcesDirectory: String = Paths.get(localDirectory, "src", "main", "resources").toString + localSeparator
  val dataDirectory: String = Paths.get(localDirectory, "data").toString + localSeparator


}
