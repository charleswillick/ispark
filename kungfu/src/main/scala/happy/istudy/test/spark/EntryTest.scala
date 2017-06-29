package happy.istudy.test.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2017/6/20.
  */
object EntryTest {
  def main(args: Array[String]) {
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    import sparkSession.implicits._
    val data = sparkSession.read.text("/user/iflyrd/work/zqwu/test.txt").as[String]
    val words = data.flatMap(value => value.split("\\s+"))
    val groupedWords = words.groupByKey(_.toLowerCase)
    val counts = groupedWords.count()
    counts.show()

    sparkSession.stop()
  }

}
