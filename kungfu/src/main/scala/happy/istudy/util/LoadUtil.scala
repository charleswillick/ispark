package happy.istudy.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


/**
 * Created by zqwu on 15/7/9.
 * 关于load的一些util
 */
object LoadUtil {
  /*
   * @input       String
   * @output    Map
   * @func         String -> Map
   */
  def MapLoader(line:String):Map[String,String] = {
    //the separator between Maps
    val sepTerm = 31.toChar.toString
    //the separator between KVs
    val sepKv = "~"
    val terms = line.split(sepTerm)
    val result = scala.collection.mutable.Map[String, String]()
    for (term <- terms){
      val kvs = term.split(sepKv, 2)
      //if key is null or value is null, then don't insert into the Map
      if (kvs.length > 1 && kvs(0).nonEmpty && kvs(1).nonEmpty){
        //key toLowerCase
        result += (kvs(0).toLowerCase() -> kvs(1))
      }
    }
    result.toMap
  }


  /**
   * 读取资源文件内容生成Map
   * @param sc
   * @param path
   * @return
   */
  def resource2Map(sc: SparkContext, path: String): Map[String, String] = {
    sc.textFile(path)
      //不为空且不以#开头
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      //默认分隔符为~
      .map(_.split("~", 2))
      .map(x => (x(0), x(1)))
      .collect()
      .toMap
  }

  def readLzoFile(sc:SparkContext,path:String):RDD[String] ={
    sc.newAPIHadoopFile(path,
      classOf[com.hadoop.mapreduce.LzoTextInputFormat],
      classOf[org.apache.hadoop.io.LongWritable],
      classOf[org.apache.hadoop.io.Text])
      .map(_._2.toString)
  }

  def readTextFile(sc:SparkContext,path:String):RDD[String] ={
    sc.textFile(path)
  }

  val LZO = "lzo"
  val TEXT = "text"

  def readFileWithType(sc:SparkContext,path:String, flag:String):RDD[String] ={
    if(flag == LZO)  {
      readLzoFile(sc, path)
    }else if (flag == TEXT){
      readTextFile(sc, path)
    }else {
      readTextFile(sc, path)
    }
  }

  /**
   * function：读取参数文件，并将内容转成map格式
   * filepath:参数文件的路径
   * return：参数
   */

  def paramfile2Map(filepath:String):Map[String,String] = {
    scala.io.Source.fromFile(filepath).getLines()
      .filter(line => !(line.isEmpty || line.startsWith("#"))) //去掉注释和空行
      .map(_.split("~", 2)).map(x => (x(0), x(1))).toMap  //参数配置是k-v格式，已~分割
  }



}
