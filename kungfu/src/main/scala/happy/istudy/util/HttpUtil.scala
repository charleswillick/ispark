package happy.istudy.util

/**
  * Created by zqwu on 2016/4/27 0027.
  */
object HttpUtil {

  def get(url: String) = scala.io.Source.fromURL(url).mkString
}
