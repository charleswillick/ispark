package happy.istudy.util

import net.liftweb.json._

/**
 * Created by Administrator on 2016/10/9.
 */
object JsonUtil {
  /**
   * 解析json字符串变为相应的类
   * @param jsonStr json字符串
   * @param t 解析失败时 返回默认的的类，该类要函数调用者传入
   * @tparam T
   * @return
   */
  def str2Json[T:Manifest](jsonStr:String, t:T):T = {
    implicit val formats = DefaultFormats
    try{
      val json = parse(jsonStr)
      val m = json.extract[T]
      m
    }catch {
      case ex:Exception =>{
        ex.printStackTrace()
        t
      }
    }
  }

}
