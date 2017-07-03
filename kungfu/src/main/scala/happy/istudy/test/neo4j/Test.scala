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

    //创建节点
    session.run("CREATE (jjhu2:Person {name:'向丽丽', born:1989}) return jjhu2")
    session.run("CREATE (jjhu2:Person {name:'张志勇', born:1989}) return jjhu2")
//
    //创建关系
//    session.run("MATCH (a:Person),(b:Person) WHERE a.name = '胡进军' AND b.name = '吴志强' CREATE (a)-[r:IS_FRIEND]->(b) RETURN r")
    session.run("MATCH (a:Person),(b:Person) WHERE a.name = '向丽丽' AND b.name = '张志勇' CREATE (a)-[r:IS_FRIEND]->(b) RETURN r")
    session.run("MATCH (a:Person),(b:Person) WHERE a.name = '向丽丽' AND b.name = '张志勇' CREATE (a)<-[r:IS_FRIEND]-(b) RETURN r")

    //建索引
//    session.run("CREATE INDEX ON :Person(name)")
    //删除有detach就会删除节点及节点上的关系，没有这不会删除关系，如果节点上有关系，则不能删除
//   session.run("match (p:Person {name:'向丽丽'}) detach delete p")
//   session.run("match (p:Person {name:'张志勇'}) detach delete p")

    //match 查询

//    val result = session.run("match (p:Person {name:'刘刚'}) return p")
//    println(result.next().get("p").asNode().get("name"))
//    println(result.next().get("p").asNode().get("name"))
    session.close()

  }

}
