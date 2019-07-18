package pro

import `trait`.Process
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import utils.{ConfigUtils, DateUtils, KuduUtils}

/**
  * 统计广告投放的app分布情况
  */
object AppAnaylysis extends Process{

  //定义数据存入表
  val SINK_TABLE = s"ad_app_analysis_${DateUtils.getNow()}"
  //定义数据原表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //定义kudu信息
  val options = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {

    //1、读取ODS表的数据
    import org.apache.kudu.spark.kudu._
    spark.read.options(options).kudu.createOrReplaceTempView("t_data_info")
    //2、统计报表
      //1、统计原始请求数、有效请求数、广告请求数、展示量、点击量、参与竞价数、竞价成功数、广告成本、广告消费
    spark.sql(
      """
        |select
        | appid,appname,
        | sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) org_num,
        | sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) valid_num,
        | sum(case when requestmode=1 and processnode=3 then 1 else 0 end) ad_num,
        | sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and isbid=1 and adorderid!=0 then 1 else 0 end) bid_num,
        | sum(case when iseffective=1 and iswin=1 and isbilling=1 and adplatformproviderid>=100000 then 1 else 0 end) bid_success_num,
        | sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) show_num,
        | sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) click_num,
        | sum(case when requestmode=2 and iseffective=1 and isbilling=1 then 1 else 0 end) media_show_num,
        | sum(case when requestmode=3 and iseffective=1 and isbilling=1 then 1 else 0 end) media_click_num,
        | sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then winprice/1000 else 0 end) consumtion_number,
        | sum(case when adplatformproviderid>=100000 and iseffective=1 and isbilling=1 and iswin=1 and adorderid>200000 and adcreativeid>200000 then adpayment/1000 else 0 end) cost_number
        |from t_data_info
        |group by appid,appname
      """.stripMargin).createOrReplaceTempView("t_tmp_info")
    //2、计算竞价成功率、点击率
    val result = spark.sql(
      """
        |select
        | appid,appname,
        | org_num,
        | valid_num,
        | ad_num,
        | bid_num,
        | bid_success_num,
        | bid_success_num/bid_num bid_success_rat,
        | show_num,
        | click_num,
        | click_num/show_num click_rat,
        | media_show_num,
        | media_click_num,
        | consumtion_number,
        | cost_number
        | from t_tmp_info
      """.stripMargin)

    //3、写入kudu
    val schema = result.schema
    val kuduOptions = new CreateTableOptions()
    //设置分区字段 分区策略 分区数
    val columns = Seq[String]("appid","appname")
    import scala.collection.JavaConverters._
    kuduOptions.addHashPartitions(columns.asJava,3)
    //设置副本数
    kuduOptions.setNumReplicas(3)
    //设置主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,kuduOptions,SINK_TABLE,result,keys)
  }
}
