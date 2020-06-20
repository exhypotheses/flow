package com.grey.trips

class InterfaceVariables {

  val startDate = "2018/09"
  val endDate = "2019/02"
  val step = 1
  val stepType = "months"
  val dateTimePattern = "yyyy/MM"

  // api.format(dateTimePattern)
  val api = "https://data.urbansharing.com/edinburghcyclehire.com/trips/v1/%s.json"

  // https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html
  val sourceTimeStamp = "yyyy-MM-dd HH:mm:ss.SSSXXX"
  val projectTimeStamp = "yyyy-MM-dd HH:mm:ss.SSS"

}
