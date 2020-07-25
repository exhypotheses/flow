package com.grey.trips.specific

class InterfaceVariables {

  // In general
  val startDate = "2018/12"
  val endDate = "2019/02"
  val step = 1
  val stepType = "months"
  val dateTimePattern = "yyyy/MM"

  // Usage: api.format(dateTimePattern)
  val api = "https://data.urbansharing.com/edinburghcyclehire.com/trips/v1/%s.json"

  // https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  val projectTimeStamp = "yyyy-MM-dd HH:mm:ss.SSS"
  val sourceTimeStamp = "yyyy-MM-dd HH:mm:ss.SSSXXXZ"

}
