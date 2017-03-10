package happy.istudy.util.etl

import happy.istudy.util.keyset.ItemKeys
import net.liftweb.json._

/**
  * Created by zqwu on 2016/4/26 0026.
  */
object CommonFormat {

  /**
    * 提取document日志中channels字段里的channelname, channelid
    * @param channelsJson
    * @return
    */
  def getChannels(channelsJson:String):Map[String,String] = {
    implicit val formats = DefaultFormats
    //case class Channels (channelid:String,parentid:String,channelname:String,desc:String)
    case class Channels (channelid:String,channelname:String)
    try{
      val json = parse(channelsJson)
      val channels = json.children
      val firstChannel = channels(0)
      val m = firstChannel.extract[Channels]
      Map(ItemKeys.CHANNEL -> m.channelname, ItemKeys.CHANNELID -> m.channelid)
    }catch {
      case ex:Exception => Map[String,String]()
    }

  }

  /**
    * 获取新闻日志中需要的字段
    * @param record ossp刷来的原始日志（flume,kafka）
    * @return 需要提取的的字段
    */
  def itemTransform(record: Map[String, String]):Map[String,String] = {
    val channel = if(record.contains("channels")) getChannels(record.get("channels").get)
    else Map[String,String]()
    val ret = Map(ItemKeys.DOCID -> record.getOrElse("docid", "00000"),
      ItemKeys.SOURCE -> record.getOrElse("sourcename","").replaceAll(":",""),
      ItemKeys.CHANNEL -> channel.getOrElse("channelname",ItemKeys.DEFAULT_CHANNEL),
      ItemKeys.TITLE -> record.getOrElse("title",""),
      ItemKeys.RSOURCENAME -> record.getOrElse("rsourcename",""),
      ItemKeys.CTM -> formatTime(record.getOrElse("ctm","")),
      ItemKeys.PUBLISHTIME -> formatTime(record.getOrElse("publishtime","")),
      ItemKeys.COMMENT -> record.getOrElse("comment",""),
      ItemKeys.UP -> record.getOrElse("up",""),
      ItemKeys.DOWN -> record.getOrElse("down",""),
      ItemKeys.LIKE -> record.getOrElse("like",""),
      ItemKeys.CTYPE -> record.getOrElse("ctype",""),
      ItemKeys.MODELJSON -> record.getOrElse("contentmodeltags",""),
      ItemKeys.SAVECONFIG -> record.getOrElse("saveconfig",""),
      ItemKeys.ALLCONTENT -> record.getOrElse("content",""),
      ItemKeys.CONTENT -> record.getOrElse("speechdetail", "").toString.replaceAll("~|\\n", " ")
    ) ++ channel
    ret
  }

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
