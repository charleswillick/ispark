package happy.istudy.util

import java.text.DecimalFormat

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vectors, Vector}
import com.github.fommil.netlib.{BLAS => NetlibBLAS, F2jBLAS}
import org.apache.spark.rdd.RDD


/**
 * Created by zqwu on 17/3/15.
 */
object VectorUtil {

  /**
   * vector转成libsvm-string类型
   * @param vec
   * @return
   */
  def libSVMVector2String(vec: Vector): String = {
    //val vec = vec1.toDense.toSparse
    if(vec.numNonzeros == 0){
      ""
    }else{
      val sb = new StringBuilder("")
      for (i <- 0 until vec.size) {
        val value = vec.apply(i)
        if (value != 0) {
          sb ++= s"${i + 1}:$value"
          sb += ' '
        }
      }
      if (sb.length >= 1) {
        sb.deleteCharAt(sb.length - 1)
      }
      sb.mkString
    }
  }

  /**
   * libsvm-string转成vector类型
   * @param str
   * @param vecFeatureNum vector的特征个数(包括0)
   * @return
   */
  def string2LibSVMVector(str: String,
                          vecFeatureNum: Int): Vector = {
    if(str.length == 0){
      Vectors.zeros(vecFeatureNum)
    }else{
      val items = str.split(' ')
      val (indices, values) = items.filter(_.nonEmpty).map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip
      val indicesArr = indices.toArray
      val valuesArr = values.toArray
      Vectors.sparse(vecFeatureNum, indicesArr, valuesArr)
    }
  }

  /**
   * libsvm-string转成Map类型
   * @param str
   * @return
   */
  def libSVMString2Map(str:String):Map[String, String] = {
    if(str.length == 0){
      Map()
    }else{
      val items = str.split(' ')
      val (indices, values) = items.filter(_.nonEmpty).map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip
      val indicesArr = indices.toArray
      val valuesArr = values.toArray
      indicesArr.map(_.toString).zip(valuesArr.map(_.toString)).toMap
    }
  }

  /**
   * vector中无效的特征全部置0
   * @param vec
   * @param validArr
   * @return
   */
  def vectorClear(vec:Vector, validArr:Array[Int] = Array()):Vector = {
    if(validArr.length == 0){
      vec
    }else{
      val vecSparse = vec.toSparse
      val size = vecSparse.size
      val vecMap = vecSparse.indices.zip(vecSparse.values).toMap
      val vecValidMap = vecMap.filterKeys(validArr.contains(_))
      Vectors.sparse(size, vecValidMap.toSeq)
    }
  }

  /**
   * x = a * x
   */
  def scal(a: Double, x: Vector): Unit = {
    x match {
      case sx: SparseVector =>
        f2jBLAS.dscal(sx.values.size, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.size, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
  }

  /**
    * x = a * x
    */
  def multiply(a: Double, x: Vector): Vector = {
    x match {
      case sx: SparseVector =>
        f2jBLAS.dscal(sx.values.size, a, sx.values, 1)
      case dx: DenseVector =>
        f2jBLAS.dscal(dx.values.size, a, dx.values, 1)
      case _ =>
        throw new IllegalArgumentException(s"scal doesn't support vector type ${x.getClass}.")
    }
    x
  }

  /**
   * x + y
   * @param x
   * @param y
   * @return
   */
  def vecAddition(x: Vector, y: Vector): Vector = {
      val yDense = y.toDense
      axpy(1.0, x, yDense)
      yDense.toSparse
  }

  /**
   * a * x + y
   * @param a
   * @param x
   * @param y
   * @return
   */
  def vecLinear(a: Double, x: Vector, y: Vector): Vector = {
      val yDense = y.toDense
      axpy(a, x, yDense)
      yDense.toSparse
  }

  /**
    * a * x + y， 当y中的key不再x中出现时，x对应的vector不衰减
    * @param a
    * @param x
    * @param y
    * @return
    */
  def vecLinear(a: Double, x: Map[String,Vector], y: Map[String,Vector]): Map[String,Vector] = {
     x ++ y.map(kv => (kv._1,if(x.contains(kv._1)) vecLinear(a,x.get(kv._1).get,kv._2) else kv._2))
  }

  /**
   * dot(x, y)
   */
  def dot(x: Vector, y: Vector): Double = {
    require(x.size == y.size,
      "BLAS.dot(x: Vector, y:Vector) was given Vectors with non-matching sizes:" +
        " x.size = " + x.size + ", y.size = " + y.size)
    (x, y) match {
      case (dx: DenseVector, dy: DenseVector) =>
        dot(dx, dy)
      case (sx: SparseVector, dy: DenseVector) =>
        dot(sx, dy)
      case (dx: DenseVector, sy: SparseVector) =>
        dot(sy, dx)
      case (sx: SparseVector, sy: SparseVector) =>
        dot(sx, sy)
      case _ =>
        throw new IllegalArgumentException(s"dot doesn't support (${x.getClass}, ${y.getClass}).")
    }
  }

  /**
   * 计算矩阵中向量与向量之间的余弦相似度
   * @param sc
   * @param matrix
   * @return
   */
  def vecMatrixCosineSimilarity(sc: SparkContext,
                                matrix: RDD[(String, Vector)]): RDD[(String, String, Double)] = {
    val bMatrix = sc.broadcast(matrix.collect())
    matrix.flatMap {
      case (id1, vec1) => {
        val cMatrix = bMatrix.value.filter(_._1 != id1)
        cMatrix.map {
          case (id2, vec2) => {
            val sim = dot(vec1, vec2) / (Vectors.norm(vec1, 2.0) * Vectors.norm(vec2, 2.0))
            (id1, id2, sim)
          }
        }
      }
    }
  }


  /**
   * 取小数点后面3位有效
   * @param num
   * @return
   */
  def numFormat(num: Double): Double = {
    val df = new DecimalFormat("#.000")
    df.format(num).toDouble
  }


  /**
   * y += a * x
   */
  private def axpy(a: Double, x: Vector, y: Vector): Unit = {
    require(x.size == y.size)
    y match {
      case dy: DenseVector =>
        x match {
          case sx: SparseVector =>
            axpy(a, sx, dy)
          case dx: DenseVector =>
            axpy(a, dx, dy)
          case _ =>
            throw new UnsupportedOperationException(
              s"axpy doesn't support x type ${x.getClass}.")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"axpy only supports adding to a dense vector but got type ${y.getClass}.")
    }
  }

  /**
   * y += a * x
   */
  private def axpy(a: Double, x: DenseVector, y: DenseVector): Unit = {
    val n = x.size
    f2jBLAS.daxpy(n, a, x.values, 1, y.values, 1)
  }

  /**
   * y += a * x
   */
  private def axpy(a: Double, x: SparseVector, y: DenseVector): Unit = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    if (a == 1.0) {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += xValues(k)
        k += 1
      }
    } else {
      var k = 0
      while (k < nnz) {
        yValues(xIndices(k)) += a * xValues(k)
        k += 1
      }
    }
  }

  /**
   * dot(x, y)
   */
  private def dot(x: DenseVector, y: DenseVector): Double = {
    val n = x.size
    f2jBLAS.ddot(n, x.values, 1, y.values, 1)
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: DenseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val nnz = xIndices.size

    var sum = 0.0
    var k = 0
    while (k < nnz) {
      sum += xValues(k) * yValues(xIndices(k))
      k += 1
    }
    sum
  }

  /**
   * dot(x, y)
   */
  private def dot(x: SparseVector, y: SparseVector): Double = {
    val xValues = x.values
    val xIndices = x.indices
    val yValues = y.values
    val yIndices = y.indices
    val nnzx = xIndices.size
    val nnzy = yIndices.size

    var kx = 0
    var ky = 0
    var sum = 0.0
    // y catching x
    while (kx < nnzx && ky < nnzy) {
      val ix = xIndices(kx)
      while (ky < nnzy && yIndices(ky) < ix) {
        ky += 1
      }
      if (ky < nnzy && yIndices(ky) == ix) {
        sum += xValues(kx) * yValues(ky)
        ky += 1
      }
      kx += 1
    }
    sum
  }

  private def f2jBLAS: NetlibBLAS = {
    if (_f2jBLAS == null) {
      _f2jBLAS = new F2jBLAS
    }
    _f2jBLAS
  }

  def dot(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Trying to dot product vectors with different lengths.")
    var sum = 0f
    val len = a.length
    var i = 0
    while (i < len) {
      sum += a(i) * b(i)
      i += 1
    }
    sum
  }

  def addInto(into: Array[Float], x: Array[Float], scale: Long = 1L): Unit = {
    require(into.length == x.length, "Trying to add vectors with different lengths.")
    val len = into.length
    var i = 0
    while (i < len) {
      into(i) += x(i) / scale
      i += 1
    }
  }

  @transient private var _f2jBLAS: NetlibBLAS = _
  @transient private var _nativeBLAS: NetlibBLAS = _
}
