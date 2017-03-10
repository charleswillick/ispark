package happy.istudy.util.mllib

import happy.istudy.util.mllib.binary._

/**
  * Created by zqwu on 2016/12/5 0005.
  */
object AUCUtil {

  def scoreAndLabels2AUC(scoreAndLabels: Array[(Double,Double)]):Double = {
    val curve = roc(scoreAndLabels) //画roc曲线
    curve.sliding(2).map(trapezoid).reduce(_ + _) //计算面积
  }

  private def trapezoid(points: Array[(Double, Double)]): Double = {
    require(points.length == 2)
    val x = points.head
    val y = points.last
    (y._1 - x._1) * (y._2 + x._2) / 2.0
  }

  def roc(scoreAndLabels: Array[(Double,Double)]): Array[(Double, Double)] = {
    val rocCurve = createCurve(scoreAndLabels,FalsePositiveRate, Recall)
    val first = Array((0.0, 0.0))
    val last = Array((1.0, 1.0))
    first ++ rocCurve ++ last
  }

  private def createCurve(scoreAndLabels: Array[(Double,Double)],
                           x: BinaryClassificationMetricComputer,
                           y: BinaryClassificationMetricComputer): Array[(Double, Double)] = {
    val confusions = scoreAndLabels2Confusions(scoreAndLabels)
    confusions.map { case (_, c) =>
      (x(c), y(c))
    }
  }

  def scoreAndLabels2Confusions(scoreAndLabels: Array[(Double,Double)]):Array[(Double, BinaryConfusionMatrix)] = {
    // Create a bin for each distinct score value, count positives and negatives within each bin,
    // and then sort by score values in descending order.
    val binnedCounts = scoreAndLabels.groupBy(_._1).map {
      case (score, iter) => {
        (score,iter.map(_._2).map(label => new BinaryLabelCounter(0L, 0L) += label).reduce(_ += _))
      }
    }.toArray.sortWith(_._1 > _._1)
    val totalCount = binnedCounts.map(_._2).reduce(_.clone += _.clone)

    val cumCount = new BinaryLabelCounter(0L,0L)
    val cumulativeCounts = binnedCounts.map{
      case (score:Double, c:BinaryLabelCounter) => {
        cumCount += c
        (score,cumCount.clone)
      }
    }

    val confusions = cumulativeCounts.map { case (score, cumCount) =>
      (score, BinaryConfusionMatrixImpl(cumCount, totalCount).asInstanceOf[BinaryConfusionMatrix])
    }
    confusions
  }

}
