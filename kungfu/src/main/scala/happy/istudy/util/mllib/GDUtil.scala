package happy.istudy.util.mllib

import org.apache.log4j.Logger

/**
 * Created by Administrator on 2016/12/23.
 * 参考：http://blog.csdn.net/acdreamers/article/details/44658631
 */
object GDUtil {
  val logger = Logger.getLogger("org.apache.spark")

  /**
   * 线性回归的批量梯度下降 BGD
   * @param data m行n列的数据,n个特征，m个样本
   * @param label m个值
   * @param stepSize 步长
   * @param numIterations 迭代次数
   * @param stopConvergence 当损失值小于这个数时，迭代结束
   * @return 返回线性回归的系数值，长度是n + 1
   */
  def Compute(
                    data:Array[Array[Double]],
                    label:Array[Double],
                    stepSize:Double,
                    stopConvergence:Double = 0.01,
                    numIterations:Int = 200):Array[Double] = {
    val n = data(0).length
    val m = data.length
    var theta = new Array[Double](n + 1) //初始化系数值为0,存储每次迭代的前的系数
    var thetaUpdate = new Array[Double](n + 1) //初始化系数值为0，存储每次迭代后的系数
    var cnt = 0
    while(cnt < numIterations){//在迭代次数内
      for(j <- 0 to n){ //更新每一个系数 n +１个系数
        var err = 0.0
        for(i <- 1 to m){ //对每个样本计算(h(x) - yi） * x(i)(j)
          val yi = label(i - 1)
          val sample = data(i - 1)
          var hx = theta(0) //初始化每次迭代的h(x)
          for(k <- 1 to n){
            hx += theta(k) * sample(k - 1)
          }
          if(j == 0){ //第一个系数在样本中都是1
            err += hx - yi
          }else{
            err += (hx - yi) * data(i - 1)(j - 1)
          }
        }
        thetaUpdate(j) = theta(j) - stepSize * err
      }
      //计算cost
      var cost = 0.0
      for(i <- 0 to m - 1){
        var fx = thetaUpdate(0)
        for(j <- 0 to n - 1) {
          fx += thetaUpdate(j + 1) * data(i)(j)
        }
        cost += (fx - label(i)) * (fx - label(i))
      }
      cost = cost * 0.5
            //交换theta和thetaUpdate
      val temp = theta
      theta = thetaUpdate
      thetaUpdate = temp
      if(cnt % 10 == 0)println(s"round $cnt : " + thetaUpdate.mkString(",") + s" and cost is $cost")
      cnt += 1 //迭代次数加1
      if(cost < stopConvergence) {
        println(s"round $cnt and cost is $cost")
        cnt = numIterations
      }
    }
    thetaUpdate
  }

  /**
   * 线性回归的批量梯度下降 BGD
   * @param data m行n列的数据,n个特征，m个样本
   * @param label m个值
   * @param stepSize 步长
   * @param numIterations 迭代次数
   * @param stopConvergence 当损失值小于这个数时，迭代结束
   * @return 返回线性回归的系数值，长度是n + 1
   */
  def batchCompute(
                    data:Array[Array[Double]],
                    label:Array[Double],
                    stepSize:Double,
                    stopConvergence:Double = 0.01,
                    numIterations:Int = 200,
                    batchSize:Int = 100):Array[Double] = {
    val n = data(0).length
    val m = data.length
    var theta = new Array[Double](n + 1) //初始化系数值为0,存储每次迭代的前的系数
    var thetaUpdate = new Array[Double](n + 1) //初始化系数值为0，存储每次迭代后的系数
    var cnt = 0
    while(cnt < numIterations){//在迭代次数内
      for(j <- 0 to n){ //更新每一个系数 n +１个系数
      var err = 0.0
        for(tempCnt <- 1 to batchSize){ //对每个样本计算(h(x) - yi） * x(i)(j)
        val i = (Math.random() * m).toInt
        val yi = label(i)
          val sample = data(i)
          var hx = theta(0) //初始化每次迭代的h(x)
          for(k <- 1 to n){
            hx += theta(k) * sample(k - 1)
          }
          if(j == 0){ //第一个系数在样本中都是1
            err += hx - yi
          }else{
            err += (hx - yi) * data(i)(j - 1)
          }
        }
        thetaUpdate(j) = theta(j) - stepSize * err
      }
      //计算cost
      var cost = 0.0
      for(i <- 0 to m - 1){
        var fx = thetaUpdate(0)
        for(j <- 0 to n - 1) {
          fx += thetaUpdate(j + 1) * data(i)(j)
        }
        cost += (fx - label(i)) * (fx - label(i))
      }
      cost = cost * 0.5
      //交换theta和thetaUpdate
      val temp = theta
      theta = thetaUpdate
      thetaUpdate = temp
      if(cnt % 1000 == 0)println(s"round $cnt : " + thetaUpdate.mkString(",") + s" and cost is $cost")
      cnt += 1 //迭代次数加1
      if(cost < stopConvergence) {
        println(s"round $cnt and cost is $cost")
        cnt = numIterations
      }
    }
    thetaUpdate
  }


  def stochasticCompute(
                    data:Array[Array[Double]],
                    label:Array[Double],
                    stepSize:Double,
                    stopConvergence:Double = 0.01,
                    numIterations:Int = 200):Array[Double] = {
    val n = data(0).length
    val m = data.length
    var theta = new Array[Double](n + 1) //初始化系数值为0,存储每次迭代的前的系数
    var thetaUpdate = new Array[Double](n + 1) //初始化系数值为0，存储每次迭代后的系数
    var cnt = 0
    while(cnt < numIterations){//在迭代次数内
      for(j <- 0 to n){ //更新每一个系数 n +１个系数
      var err = 0.0
      val i = (Math.random() * m).toInt
      val yi = label(i)
        val sample = data(i)
        var hx = theta(0) //初始化每次迭代的h(x)
        for(k <- 1 to n){
          hx += theta(k) * sample(k - 1)
        }
        if(j == 0){ //第一个系数在样本中都是1
          err = hx - yi
        }else{
          err = (hx - yi) * data(i)(j - 1)
        }
        thetaUpdate(j) = theta(j) - stepSize * err
      }
      //计算cost
      var cost = 0.0
      for(i <- 0 to m - 1){
        var fx = thetaUpdate(0)
        for(j <- 0 to n - 1) {
          fx += thetaUpdate(j + 1) * data(i)(j)
        }
        cost += (fx - label(i)) * (fx - label(i))
      }
      cost = cost * 0.5
      //交换theta和thetaUpdate
      val temp = theta
      theta = thetaUpdate
      thetaUpdate = temp
      if(cnt % 10 == 0)println(s"round $cnt : " + thetaUpdate.mkString(",") + s" and cost is $cost")
      cnt += 1 //迭代次数加1
      if(cost < stopConvergence) {
        println(s"round $cnt and cost is $cost")
        cnt = numIterations
      }
    }
    thetaUpdate
  }

}
