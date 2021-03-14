package com.grey.trips.functions

import java.io.{File, PrintWriter}

import com.grey.trips.environment.LocalSettings
import org.apache.spark.sql.Row

/**
  * Creates HighCharts candle sticks JSON files
  */
class CandleJSON {

  private val localSettings = new LocalSettings()

  /**
    *
    * @param points   the candle sticks data points; each member of Array[Candle] must be of the
    *                 form case class Candle() outlined at the end of this program.
    * @param fileName the name of the file wherein the quantiles will be stored; exclude extensions.
    */
  def candleJSON(points: Array[Row], fileName: String): Unit = {

    val fileObject = new PrintWriter(new File(localSettings.localWarehouse + fileName + ".json"))

    // Starting
    fileObject.println("[")

    // i: row counter
    var i = 0


    // Points
    points.foreach { point =>

      // Structure
      if (i > 0) {
        fileObject.append(",")
      }

      // Data
      fileObject.println(
        "[" + point.getAs[Long]("epoch") + "," + point.getAs[Float]("lowerWhisker") + "," +
          point.getAs[Float]("lowerQuartile") + "," + point.getAs[Float]("median") + "," +
          point.getAs[Float]("upperQuartile") + "," + point.getAs[Float]("upperWhisker") +
          "," + point.getAs[Long]("tally") + "]"
      )

      // ... iteration number
      i += 1
    }

    fileObject.print("]")

    fileObject.close()

  }


}
