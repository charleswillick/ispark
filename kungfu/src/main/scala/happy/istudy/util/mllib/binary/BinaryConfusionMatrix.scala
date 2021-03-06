package happy.istudy.util.mllib.binary

/**
  * Created by zqwu on 2016/12/5 0005.
  */
/**
  * Trait for a binary confusion matrix.
  */
private[mllib] trait BinaryConfusionMatrix {
  /** number of true positives */
  def numTruePositives: Long

  /** number of false positives */
  def numFalsePositives: Long

  /** number of false negatives */
  def numFalseNegatives: Long

  /** number of true negatives */
  def numTrueNegatives: Long

  /** number of positives */
  def numPositives: Long = numTruePositives + numFalseNegatives

  /** number of negatives */
  def numNegatives: Long = numFalsePositives + numTrueNegatives
}

/**
  * Implementation of [[org.apache.spark.mllib.evaluation.binary.BinaryConfusionMatrix]].
  *
  * @param count label counter for labels with scores greater than or equal to the current score
  * @param totalCount label counter for all labels
  */
private[mllib] case class BinaryConfusionMatrixImpl(
                                                          count: BinaryLabelCounter,
                                                          totalCount: BinaryLabelCounter) extends BinaryConfusionMatrix {

  /** number of true positives */
  override def numTruePositives: Long = count.numPositives

  /** number of false positives */
  override def numFalsePositives: Long = count.numNegatives

  /** number of false negatives */
  override def numFalseNegatives: Long = totalCount.numPositives - count.numPositives

  /** number of true negatives */
  override def numTrueNegatives: Long = totalCount.numNegatives - count.numNegatives

  /** number of positives */
  override def numPositives: Long = totalCount.numPositives

  /** number of negatives */
  override def numNegatives: Long = totalCount.numNegatives
}
