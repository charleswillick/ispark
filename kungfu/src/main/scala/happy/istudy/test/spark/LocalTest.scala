package happy.istudy.test.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/6/20.
  */
case class Person(index:Int,uid:Int)
object LocalTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    import sparkSession.implicits._
    val data = sparkSession.createDataset(
      Seq(
        (1,1,Map(1 -> 2, 3 -> 5)),
        (1,2,Map(2 -> 2, 3 -> 5)),
        (1,1,Map(5 -> 2, 3 -> 5)),
        (1,3,Map(4 -> 2, 3 -> 5)),
        (1,4,Map(1 -> 2, 3 -> 5)),
        (1,3,Map(1 -> 2, 3 -> 5)),
        (1,2,Map(1 -> 2, 3 -> 5)),
        (1,2,Map(1 -> 2, 3 -> 5)),
        (2,1,Map(1 -> 2, 3 -> 5)),
        (2,2,Map(1 -> 2, 3 -> 5)),
        (2,1,Map(1 -> 2, 3 -> 5)),
        (2,2,Map(1 -> 2, 3 -> 5)),
        (3,4,Map(1 -> 2, 3 -> 5)),
        (3,3,Map(1 -> 2, 3 -> 5)),
        (3,2,Map(1 -> 2, 3 -> 5)),
        (3,2,Map(1 -> 2, 3 -> 5))
    )
    )
    data.write.mode("overwrite").format("parquet").option("compression", "gzip").save("/user/iflyrd/work/zqwu/temp/ParquetTuple")

    sparkSession.stop()
  }

}
