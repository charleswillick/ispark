package happy.istudy.test.spark

import org.apache.spark.sql.SparkSession

object ParquetWrite {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    import sparkSession.implicits._
    val data = sparkSession.createDataset(
      Seq(
        (1,1),
        (1,2),
        (1,1),
        (1,3),
        (1,4),
        (1,3),
        (1,2),
        (1,2)
    )
    )
    data.write.mode("overwrite").format("parquet").option("compression", "gzip").save("/user/iflyrd/work/zqwu/temp/ParquetTuple")

    sparkSession.stop()
  }

}
