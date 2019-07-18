package `trait`

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession

/**
  * 公共介质
  */
trait Process {
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */
  def process(spark:SparkSession,kuduContext:KuduContext):Unit
}
