package com.grey.trips.hive

import java.sql.Date

import com.grey.trips.environment.ConfigParameters
import com.grey.libraries.hive.HiveLayerCaseClass


class HiveLayerProperties {


  private val configParameters = new ConfigParameters()


  def hiveLayerProperties(date: Date, partition: String): HiveLayerCaseClass#HLCC = {

    // Specific
    val partitionValues: Map[String, String] = Map("starting" -> date.toString)

    // Case -> ALTER TABLE (specific)
    val partitionSpecification: String = configParameters.partitionVariables.keys.map { key =>
      key + " = '" + partitionValues(key) + "'"
    }.mkString(", ")

    // Hence
    new HiveLayerCaseClass().HLCC(partitionOfInterest = partition,
      partitionSpecification = partitionSpecification)

  }

}
