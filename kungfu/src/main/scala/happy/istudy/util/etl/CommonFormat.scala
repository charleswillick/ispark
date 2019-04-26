package happy.istudy.util.etl

import net.liftweb.json._

/**
  * Created by zqwu on 2016/4/26 0026.
  */
object CommonFormat {

  def formatTime(param:String):String={
    val rparam = param.replaceAll("[^\\d]", "")
    if(rparam.length >= 14) rparam.substring(0,14) else rparam + "0" * (14 - rparam.length)
  }

  /**
   * 判断一个字符串是否是有中文和ASCII码为0-128的字符组成
   * @param str
   * @return
   */
  def isCommonString(str:String):Boolean={
    str.matches("[\\x00-\\x7F\\u4e00-\\u9fa5]+")
  }

  /**
   * 判断一个字符串是否是有中文、英文字母及数字组成
   * @param str
   * @return
   */
  def isCommonWord(str:String):Boolean={
    str.matches("a-z0-9A-Z\\u4e00-\\u9fa5]+")
  }

}
