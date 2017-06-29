package happy.istudy.test.spark

import org.apache.spark.sql.SparkSession

object ParquetRead {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    import sparkSession.implicits._
    val data = sparkSession.read.parquet("/user/iflyrd/work/zqwu/temp/ParquetTuple").as[(Int,Int,Map[Int,Int])]
    val t  = data.flatMap(x => x._3.map(t => ((t._1,x._2),t._2)))
      .groupByKey(_._1)
      .mapGroups((key,iter) => {
        val value = iter.map(_._2).reduce(_ + _)
        (key,value)
      }).map{
      case ((index,uid),cnt) => (index,(1,cnt))
    }.groupByKey(_._1).mapGroups((key,iter) => {
      val value = iter.map(_._2).reduce((x,y) => (x._1 + y._1,x._2 + y._2))
      (key,value)
    }).show()

    sparkSession.stop()
  }

}
