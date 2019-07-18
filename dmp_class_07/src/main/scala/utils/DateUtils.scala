package utils

import java.util.{Calendar, Date}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期生成帮助类
  */
object DateUtils {

  /**
    * 获取当天日志yyyyMMdd格式的字符串
    * @return
    */
  def getNow():String={

    //20190615
    val date = new Date()

    val format = FastDateFormat.getInstance("yyyyMMdd")

    format.format(date)
  }

  /**
    * 获取昨天日期的yyyyMMdd格式字符串
    * @return
    */
  def getYesterDay():String ={
    val date = new Date()

    val calendar = Calendar.getInstance()

    calendar.setTime(date)

    calendar.add(Calendar.DAY_OF_YEAR,-1)

    val format = FastDateFormat.getInstance("yyyyMMdd")

    format.format(calendar)
  }
}
