package happy.istudy.util

import com.typesafe.config.Config
/**
 * Created by Administrator on 2015/12/11.
 */
object ConfigUtil {
  implicit def convertConfig(conf:Config) = new ConfigSimple(conf)
  class ConfigSimple(conf:Config){
    /**
     * 对每个key进行相应的func操作
     * @param func
     */
    def foreachEntry(func:String => Any): Unit ={
      keySet().foreach(func(_))
    }

    /**
     * 得到一个config的所有key
     * @return
     */
    def keySet():Set[String] = {
      val configIt = conf.entrySet.iterator
      var ret: Set[String] = Set()
      while (configIt.hasNext) {
        val key = configIt.next.getKey
        ret = ret + key.split("\\.")(0)
      }
      ret
    }
  }
}
