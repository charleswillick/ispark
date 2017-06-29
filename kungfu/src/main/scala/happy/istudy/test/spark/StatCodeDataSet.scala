package happy.istudy.test.spark

import breeze.linalg.sum
import happy.istudy.storage.hbase.HBaseUtil
import happy.istudy.util.SparkContextUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Administrator on 2017/6/20.
  */
object StatCodeDataSet {
  def main(args: Array[String]) {
    val date = args(0)
    val sparkSession = SparkSession.builder().appName(this.getClass.getName).getOrCreate()
    import sparkSession.implicits._
    val dataSet = sparkSession.read.text("/user/iflytd/gangliu6/ossp/inter/EtlSys/StatLog/20170713").as[String]
      .map(_.split("~",-1))
      .map(x => (x(0),x(1),x(2),x(4),x(18))) //uid,bizid,osid,version,stat

    //rdd.cache()
    //bizid, osid,0.0.0001,0000 pv, uv
    val cube01 = stat(sparkSession,dataSet.map(x => (Seq(x._2, x._3,date, "0.0.0001", "0000"), x._1)))
    //bizid, osid, version, 0000, pv, uv
    val cube02 = stat(sparkSession,dataSet.map(x => (Seq(x._2, x._3,date, x._4, "0000"), x._1)))

    //bizid, osid, version,statcode pv, uv
    val statData =  dataSet.filter(_._5.contains("#"))
    val statMapver = statData.map(x => {
      val len = x._5.length
      ((x._1,x._2,x._3,x._4),x._5.substring(1,len -1).split(",").map(t => (t.split("#",2)(0),t.split("#")(1).toLong)))
    }).groupByKey(_._1)
      .mapGroups((key,value) =>
        (key,value.map(_._2).reduce((x,y) => mapAdd(x,y)))
      )

    statMapver.cache()
    val statMap = statMapver.map(x => ((x._1._1,x._1._2,x._1._3),x._2))
      .groupByKey(_._1)
      .mapGroups((key,value) =>
        (key,value.map(_._2).reduce((x,y) => mapAdd(x,y)))
      )
    val cube03 = statMapver.map(x => (Seq(x._1._2,x._1._3,date,x._1._4),(x._2.map(t => (t._1, 1.toLong ,t._2)))))
      .groupByKey(_._1)
      .mapGroups((key,value) =>
        (key,value.map(_._2).reduce((x,y) => mapAdd(x,y)))
      )
      .flatMap(x => x._2.map(t => (x._1 ++ Seq(t._1),(t._2,t._3))))
    val cube04 = statMap.map(x => (Seq(x._1._2,x._1._3,date,"0.0.0001"),(x._2.map(t => (t._1, 1.toLong ,t._2)))))
      .groupByKey(_._1)
      .mapGroups((key,value) =>
        (key,value.map(_._2).reduce((x,y) => mapAdd(x,y)))
      )
      .flatMap(x => x._2.map(t => (x._1 ++ Seq(t._1),(t._2,t._3))))

    statMapver.unpersist()
    dataSet.write.mode("out").format("").option("","").save("")
    //rdd.unpersist()
    val result = cube01.union(cube02).union(cube03).union(cube04).map(x => (x._1.mkString("~"),Map("cf:DAU" -> x._2._1.toString,"cf:DFC" -> x._2._2.toString)))
    HBaseUtil.setHBaseConf("192.168.45.150")
    HBaseUtil.hbaseWrite("iflytd:OsspDataAnaSysDayStatCode",result.rdd)

    sparkSession.stop()

  }

  /**
    * 在Seq的维度下统计人数次数
 *
    * @param rdd （维度，uid）
    * @return
    */
  def stat(spark:SparkSession,rdd: Dataset[(Seq[String],String)]):Dataset[(Seq[String],(Long, Long))]={
    import spark.implicits._
    val ret = rdd.map(x => (x,1L))
      .groupByKey(_._1)
      .mapGroups((key,value) => (key,value.map(_._2).reduce(_ + _))).map {
      case ((seq, uid), cnt) => (seq, (1L, cnt))
    }.groupByKey(_._1)
      .mapGroups((key,value) =>{
        val v = value.map(_._2).reduce((x,y) => (x._1 + y._1, x._2 + y._2))
        (key,v)
      }
      )
    ret
  }

  def mapAdd(x:Array[(String,Long,Long)],y:Array[(String,Long,Long)]):Array[(String,Long,Long)] ={
    (x ++ y).groupBy(_._1).map(k => {
      val (uv, pv) = k._2.map(s => (s._2,s._3)).reduce((p,q) => (p._1 + q._1,q._2 + p._2))
      (k._1,uv, pv)
    }).toArray
  }

  def mapAdd(x:Array[(String,Long)],y:Array[(String,Long)]):Array[(String,Long)] ={
    (x ++ y).groupBy(_._1).map(k => {
      val cnt = k._2.map(_._2).reduce(_ + _)
      (k._1,cnt)
    }).toArray
  }

}
