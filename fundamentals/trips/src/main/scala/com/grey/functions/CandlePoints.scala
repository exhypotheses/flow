package com.grey.functions

import java.io.{File, PrintWriter}

import com.grey.directories.LocalSettings

/**
  * Creates HighCharts candle sticks JSON files
  */
class CandlePoints {


  private val localSettings = new LocalSettings()

  /**
    *
    * @param points the candle sticks data points; each member of Array[Candle] must be of the
    *               form case class Candle() outlined at the end of this program.
    * @param fileName the name of the file wherein the quantiles will be stored; exclude extensions.
    */
  def candlePoints(points: Array[Candle], fileName: String): Unit = {

    val fileObject = new PrintWriter(new File(localSettings.dataDirectory + fileName + ".json"))

    // Starting
    fileObject.println("[")

    // i: row counter
    var i = 0


    // Points
    points.foreach{point =>

      // Structure
      if (i > 0){
        fileObject.append(",")
      }

      // Data
      fileObject.println("[" + point.epoch + "," + point.lowerWhisker + "," + point.lowerQuartile + "," +
        point.median + "," + point.upperQuartile + "," + point.upperWhisker + "," + point.tally + "]")

      // ... iteration number
      i += 1
    }

    fileObject.print("]")

    fileObject.close()

  }

  case class Candle(epoch: Long, lowerWhisker: Double, lowerQuartile: Double,
                    median: Double, upperQuartile: Double, upperWhisker: Double, tally: Long)

}
