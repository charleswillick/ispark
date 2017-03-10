package happy.istudy.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
 * Created by Administrator on 2015/8/4.
 */
object TimeFunction {

  def transformat(str:String,formatSrc:String,formatDes:String):String = {
    val dfSrc = new SimpleDateFormat(formatSrc)
    val dfDes = new SimpleDateFormat(formatDes)
    val dt = dfSrc.parse(str)
    dfDes.format(dt)
  }

  def getTimeStamp(str:String,format:String = "yyyyMMdd"):String = {
    val df = new SimpleDateFormat(format)
    val dt = df.parse(str)
    dt.getTime.toString
  }

  /**
   *
   * param 输入时间 20150101格式
   * param 间隔天数
   * return 结果 20150101格式
   */
  def subDays(str:String,range:Long):String ={
    val fstr = str.replaceAll("-","").substring(0,8)
    val df = new SimpleDateFormat("yyyyMMdd")
    val dt = df.parse(fstr)
    df.format(new Date(dt.getTime - 86400000 * range))
  }

  /**
   *
   * param 输入时间 20150101格式
   * param 间隔天数
   * return 结果 20150101格式
   */
  def addDays(str:String,range:Long,format:String = "yyyyMMdd"):String ={
    val df = new SimpleDateFormat(format)
    val dt = df.parse(str)
    df.format(new Date(dt.getTime + 86400000 * range))
  }

  /**
   *
   * param 输入时间 20150101格式
   * param 间隔天数
   * return 结果 20150201格式
   */
  def addMonth(str:String,range:Long,format:String = "yyyyMMdd"):String ={
    val df = new SimpleDateFormat(format)
    val dt = df.parse(str)
    val month = dt.getMonth + range
    dt.setMonth(month.toInt)
    df.format(dt)
  }

/**
   * 计算 last和pre的相隔天数
   * param pre 
   * param last 
   * return 结果 20150101格式
   */
  def diffDate(pre:String,last:String,format:String = "yyyyMMdd"):Long ={
    val df = new SimpleDateFormat(format)
    val diff = df.parse(last).getTime - df.parse(pre).getTime
    diff / 86400000 //(24 * 60 * 60 * 1000)
  }


  /**
   * 计算 last和pre的相隔毫秒数
   * param pre
   * param last
   * return 结果 20150101010101000格式
   */
  def diffTime(pre:String,last:String,format:String = "yyyyMMddHHmmssSSS"):Long ={
    val df = new SimpleDateFormat(format)
    val diff = df.parse(last).getTime - df.parse(pre).getTime
    diff
  }

  /**
   * 计算 last和pre的相隔秒数
   * param pre
   * param last
   * return 结果 20150101010101格式
   */
  def diffSecond(pre:String,last:String,format:String = "yyyyMMddHHmmss"):Long ={
    diffTime(pre,last,format) / 1000
  }

  /**
   * 计算 last和pre的相隔小时数
   * param pre
   * param last
   * return 结果 20150101010101格式
   */
  def diffHour(pre:String,last:String,format:String = "yyyyMMddHHmmss"):Long ={
    diffTime(pre,last,format) / 3600000
  }



  /**
   * 计算 last和pre的相隔天数
   * param pre
   * param last
   * return 结果 20150101格式
   */
  def diffMonth(pre:String,last:String,format:String = "yyyyMMdd"):Long ={
    val df = new SimpleDateFormat(format)
    val pdate = df.parse(pre)
    val ldate = df.parse(last)
    ldate.getMonth - pdate.getMonth  + (ldate.getYear - pdate.getYear) * 12
  }

  def getCurrentTime(format:String = "yyyyMMddHHmmss"):String = {
    val today = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat(format)
    formatter.format(today)
  }

  def getMonday(date:String):String = {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    cal.setTime(sdf.parse(date))
    if (1 == cal.get(Calendar.DAY_OF_WEEK)) cal.add(Calendar.DAY_OF_MONTH, -1)
    cal.setFirstDayOfWeek(Calendar.MONDAY)
    cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - cal.get(Calendar.DAY_OF_WEEK))
    sdf.format(cal.getTime())
  }

  def getFriday(date:String):String = {
    val cal = Calendar.getInstance()
    val sdf = new SimpleDateFormat("yyyyMMdd")
    cal.setTime(sdf.parse(date))
    if (1 == cal.get(Calendar.DAY_OF_WEEK)) cal.add(Calendar.DAY_OF_MONTH, -1)
    cal.setFirstDayOfWeek(Calendar.FRIDAY)
    cal.add(Calendar.DATE, cal.getFirstDayOfWeek() - cal.get(Calendar.DAY_OF_WEEK))
    sdf.format(cal.getTime())
  }

}
