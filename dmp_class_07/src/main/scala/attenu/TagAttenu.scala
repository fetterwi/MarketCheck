package attenu

import org.apache.spark.rdd.RDD
import utils.ConfigUtils

/**
  * 标签衰减
  */
object TagAttenu {

  def attenu(historyData:RDD[(String, (List[String], Map[String, Double]))]):RDD[(String, (List[String], Map[String, Double]))]={

    historyData.map{
      case (userId,(allUserIds,tags))=>{

        val historyTags = tags.map{
          //当前标签权重 = 历史标签权重 * 衰减系数
          case (tagName,attr) => (tagName,attr * ConfigUtils.ATTENU.toDouble)
        }

        (userId,(allUserIds,historyTags))
      }
    }
  }
}
