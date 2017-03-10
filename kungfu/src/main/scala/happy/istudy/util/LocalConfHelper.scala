package happy.istudy.util

import java.io.File

import com.typesafe.config.ConfigFactory

/**
 * Created by zqwu on 2017/8/13.
 */
object LocalConfHelper {
  def getConfig(confPath:String): com.typesafe.config.Config ={
    val confFile = ConfigFactory.parseFile(new File(confPath))
    ConfigFactory.load(confFile)
  }
}
