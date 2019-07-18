package graph

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * 用户统一识别
  */
object UserGraph {

  def graph(currentDayRdd: RDD[(String, (List[String], Map[String, Double]))]):RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))]={

    //1、构建点
    val vertices: RDD[(Long, (List[String],Map[String, Double]))] = currentDayRdd.map {
      case (userid, (allUserIds, tags)) => {
        (userid.hashCode.toLong, (allUserIds,tags))
      }
    }
    //2、构建边
    val edges = currentDayRdd.flatMap {
      case (userid, (allUserIds, tags)) => {
        var result = List[Edge[Int]]()

        allUserIds.foreach(user => {
          result = result.:+(Edge(userid.hashCode.toLong, user.hashCode.toLong, 0))
        })

        result
      }
    }

    //3、构建图
    val graph: Graph[(List[String], Map[String, Double]), Int] = Graph(vertices,edges)
    //4、构建连通图
    val components: Graph[VertexId, Int] = graph.connectedComponents()
    //5、获取用户信息 (id,aggid)
    val componentVertices: VertexRDD[VertexId] = components.vertices
    //(id,aggid)  join (id,(allid,tags))  => (id,(aggid,(allid,tags)))
    val value: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))] = componentVertices.join(vertices)
    //6、数据返回
    value
  }
}
