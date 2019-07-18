package merge

import agg.TagAgg
import attenu.TagAttenu
import graph.UserGraph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD

/**
  * 当天标签数据与历史标签数据的合并
  */
object TagMerge {
  /**
    * 问题:
    * 1、只有当天的标签数据
    * 2、当天数据与历史数据合并问题:
    *   1、历史标签权重如果处理
    *   2、合并后出现一个用户多条记录的情况
    *   3、历史数据与当天数据中出现相同标签怎么处理
    *
    * 解决方案:
    *   1、与历史数据合并
    *   2、历史标签权重如果处理：使用牛顿冷却定律==> 当前温度 = 初始温度 * exp(-冷却系数 * 时间间隔)
    *      标签衰减公式 ==> 当前权重 = 历史权重 * 衰减系数(自定为0.8)
    *   3、用用户统一识别来解决历史数据与当天数据合并后出现一个用户出现多条记录的情况
    *   4、标签聚合
    * @param historyData
    * @param currentData
    */
  def merge(historyData:RDD[(String, (List[String], Map[String, Double]))],currentData:RDD[(String, (List[String], Map[String, Double]))]):RDD[(String, (List[String], Map[String, Double]))]={

    //1、历史数据标签衰减
    val historyTagRdd: RDD[(String, (List[String], Map[String, Double]))] = TagAttenu.attenu(historyData)
    //2、历史数据与当天数据的合并
    val tagsRdd: RDD[(String, (List[String], Map[String, Double]))] = historyTagRdd.union(currentData)
    //3、用户统一识别
    val graph: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))] = UserGraph.graph(tagsRdd)
    //4、标签聚合
    val resultTags: RDD[(VertexId, (List[String], Map[String, Double]))] = TagAgg.agg(graph)

    resultTags.map{
      case (aggid,(allUserIds,tags)) => (allUserIds.head,(allUserIds,tags))
    }
  }
}
