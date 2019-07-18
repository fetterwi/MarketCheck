package pro

import `trait`.Process
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import utils.{ConfigUtils, DateUtils, KuduUtils}

/**
  * 统计各省市的地域分布情况
  */
object ProviceCityAnalysis extends Process{
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */
  //指明数据读取表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //指明kudu的信息
  val options = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  //指明指标数据保存表
  val SINK_TABLE = s"provice_city_${DateUtils.getNow()}"
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、从ODS表获取
    import org.apache.kudu.spark.kudu._
    spark.read.options(options).kudu.createOrReplaceTempView("t_data_info")
    //2、报表统计
    val result = spark.sql(
      """
        |select proviceName,city,count(1)
        | from t_data_info
        | group by proviceName,city
      """.stripMargin)
    //3、写入kudu
    //定义schema信息
    val schema = result.schema

    //定义表的属性
    val option = new CreateTableOptions()

    //设置分区策略 分区字段 分区数
    val columns = Seq[String]("proviceName","city")
    import scala.collection.JavaConverters._
    option.addHashPartitions(columns.asJava,3)
    //设置副本数
    option.setNumReplicas(3)

    //指定主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,option,SINK_TABLE,result,keys)
  }
}
