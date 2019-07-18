package agg

import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
  * 标签聚合
  */
object TagAgg {

  def agg(graphRdd: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))]):RDD[(VertexId, (List[String], Map[String, Double]))]={

    val aggRdd = graphRdd.map {
      case (userId, (aggid, (allUserIds, tags))) =>
        (aggid, (allUserIds, tags))
    }
    //[(1,(userIds,tags)),(1,(userIds,tags)),(1,(userIds,tags))] .reduceByKey
    val result: RDD[(VertexId, (List[String], Map[String, Double]))] = aggRdd.reduceByKey((left, right) => {

      val leftUserIds = left._1

      val rightUserIds = right._1

      //用户所有标识
      val allUserIds = (leftUserIds ++ rightUserIds).distinct

      val leftTags = left._2

      val rightTags = right._2
      //[(BA_永昌,1.0),(BA_永昌,1.0)] =>[(BA_永昌,[(BA_永昌,1.0),(BA_永昌,1.0)])]
      val groupTags: Map[String, List[(String, Double)]] = (leftTags.toList ++ rightTags.toList).groupBy(_._1)

      val allTags: Map[String, Double] = groupTags.map {
        case (tagName, tags) =>
          (tagName, tags.map(_._2).sum)
      }
      (allUserIds, allTags)
    })
    result
  }
}
