package happy.istudy.util.mllib.binary

/**
  * Created by zqwu on 2016/12/5 0005.
  */
/**
  * Trait for a binary classification evaluation metric computer.
  */
private[mllib] trait BinaryClassificationMetricComputer extends Serializable {
  def apply(c: BinaryConfusionMatrix): Double
}

/** Precision. Defined as 1.0 when there are no positive examples. */
private[mllib] object Precision extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    val totalPositives = c.numTruePositives + c.numFalsePositives
    if (totalPositives == 0) {
      1.0
    } else {
      c.numTruePositives.toDouble / totalPositives
    }
  }
}

/** False positive rate. Defined as 0.0 when there are no negative examples. */
private[mllib] object FalsePositiveRate extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    if (c.numNegatives == 0) {
      0.0
    } else {
      c.numFalsePositives.toDouble / c.numNegatives
    }
  }
}

/** Recall. Defined as 0.0 when there are no positive examples. */
private[mllib] object Recall extends BinaryClassificationMetricComputer {
  override def apply(c: BinaryConfusionMatrix): Double = {
    if (c.numPositives == 0) {
      0.0
    } else {
      c.numTruePositives.toDouble / c.numPositives
    }
  }
}

/**
  * F-Measure. Defined as 0 if both precision and recall are 0. EG in the case that all examples
  * are false positives.
  * @param beta the beta constant in F-Measure
  * @see http://en.wikipedia.org/wiki/F1_score
  */
private[mllib] case class FMeasure(beta: Double) extends BinaryClassificationMetricComputer {
  private val beta2 = beta * beta
  override def apply(c: BinaryConfusionMatrix): Double = {
    val precision = Precision(c)
    val recall = Recall(c)
    if (precision + recall == 0) {
      0.0
    } else {
      (1.0 + beta2) * (precision * recall) / (beta2 * precision + recall)
    }
  }
}
