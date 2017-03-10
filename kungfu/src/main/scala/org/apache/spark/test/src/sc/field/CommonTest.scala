package org.apache.spark.test.src.sc.field

import happy.istudy.util.SparkContextUtil

/**
 * Created by Administrator on 2017/2/14.
 */
object CommonTest {
  def main(args: Array[String]) {
    val sc = SparkContextUtil.createSparkContext("local",this.getClass.getName)
    println(sc.sparkUser)
    val iter = sc.hadoopConfiguration.iterator()
    while(iter.hasNext){
      println(iter.next())
    }

  }

}
