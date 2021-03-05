package com.grey.trips.functions

class CandleClass {

  case class Candle(epoch: Long, lowerWhisker: Double, lowerQuartile: Double,
                    median: Double, upperQuartile: Double, upperWhisker: Double, tally: Long)

}
