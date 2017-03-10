package org.apache.spark.test.src.sc.field

import happy.istudy.util.SparkContextUtil

/**
 * Created by Administrator on 2017/2/14.
 */
object creationSite {
  def main(args: Array[String]) {
    val sc = SparkContextUtil.createSparkContext("local",this.getClass.getName)
    sc.stop()
    val data = sc.parallelize(Seq(1,2))
    data.collect().foreach(println)
  }

}
