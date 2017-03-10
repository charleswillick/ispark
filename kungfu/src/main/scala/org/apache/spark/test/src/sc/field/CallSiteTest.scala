package org.apache.spark.test.src.sc.field

import org.apache.spark.util._

/**
 * Created by Administrator on 2017/2/14.
 */
object CallSiteTest {
  def main(args: Array[String]) {
    val creationSite = Utils.getCallSite()
    println(creationSite.shortForm)
    println("========================")
    println(creationSite.longForm)
  }

}
