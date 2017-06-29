package happy.istudy.test.neo4j

import org.neo4j.driver.v1.{AuthTokens, GraphDatabase}

/**
  * Created by Administrator on 2017/6/28.
  */
object Test {
  def main(args: Array[String]) {
    val driver = GraphDatabase.driver("bolt://192.168.45.24:7687", AuthTokens.basic("neo4j", "uba" ) )
    val session = driver.session()

    //查找
//    val user = session.run("start user=node(0) return user")
//    val name = session.run("start user=node(1) return user.id As name")
//    val lst = user.next().get("user").asNode().labels()
//    val nstr = name.next().get("name").asString()
//    println(lst)
//    println(nstr)

    //创建
//    session.run("CREATE (jjhu2:Person {name:'胡进军', born:1989})")

    //建索引
//    session.run("CREATE INDEX ON :Person(name)")
    //删除
//   session.run("start n = node:Person(name = '刘刚') delete n")

    //match 查询

    val result = session.run("match (p:Person) where p.name = '刘刚' return p")
    println(result.next().get("p").asNode().get("name"))
    println(result.next().get("p").asNode().get("name"))
    session.close()

  }

}
