package happy.istudy.util

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by zqwu on 2017/3/15.
 */
object SparkContextUtil {
  def createSparkContext(mode:String, appName:String):SparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
    if (mode == "local") {
      sparkConf.setMaster("local[4]")
     }
    new SparkContext(sparkConf)
  }
}
