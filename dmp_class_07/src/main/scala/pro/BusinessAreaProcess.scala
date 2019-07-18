package pro

import java.util

import `trait`.Process
import bean.BusinessArea
import ch.hsr.geohash.GeoHash
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import utils.{ConfigUtils, DateUtils, HttpUtils, KuduUtils}
import scala.collection.JavaConverters._
import scala.util.Try

/**
  * 生成商圈库
  */
object BusinessAreaProcess extends Process{
  //定义原表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //定义kudu信息
  val options = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  //定义数据存入的表名
  val SINK_TABLE = s"business_area_${DateUtils.getNow()}"
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {

    //1、获取ODS表的数据
    import org.apache.kudu.spark.kudu._
    val odsDF = spark.read.options(options).kudu
    //过滤不符合要求的数据
    val tudeDF: Dataset[Row] = odsDF.filter("longitude is not null and latitude is not null")

    //2、取出经纬度，请求高德服务获取商圈
    //去重经纬度
    val distinctDF = tudeDF.selectExpr("latitude","longitude").distinct()
    val tudeRdd = distinctDF.rdd
    val areasRdd = tudeRdd.map(row => {
      //经度
      val longitude = row.getAs[Float]("longitude")
      //纬度
      val latitude = row.getAs[Float]("latitude")
      //拼接经纬度
      val longLat = s"${longitude},${latitude}"
      //获取服务URL
      val url = ConfigUtils.URL.format(longLat)
      //发起请求，获取数据
      val response: String = HttpUtils.get(url)
      //获取商圈列表,多个商圈以,分割
      val areas = parseJson(response)
      //将经纬度进行编码
      val genhash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble, longitude.toDouble, 8)

      (genhash, areas)
    })

    import spark.implicits._
    val areasDF = areasRdd.toDF("genhash","areas")

    //过滤出商圈列表为空的数据
    val result = areasDF.filter("areas!=''")
    //3、写入kudu
    //指定schema
    val schema = result.schema
    //指定表的属性 分区策略 分区数  分区字段
    val kuduoptions = new CreateTableOptions()
    //分区字段
    val columns = Seq[String]("genhash")

    kuduoptions.addHashPartitions(columns.asJava,3)

    //指定副本数
    kuduoptions.setNumReplicas(3)
    //指定主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,kuduoptions,SINK_TABLE,result,keys)
  }

  /**
    * scala 方式解析json
    * @param json
    * @return
    */
  def parseJson(json:String):String={

    import scala.collection.JavaConverters._
    Try(JSON.parseObject(json)
      .getJSONObject("regeocode")
      .getJSONObject("addressComponent")
      .getJSONArray("businessAreas")
      .toJavaList(classOf[BusinessArea])
      .asScala
      .map(_.name)
      .mkString(",")).getOrElse("")
  }

  //xx,xx  商圈,商圈
  /*def parseJson(json:String):String={

    val obj: JSONObject = JSON.parseObject(json)

    var result = ""
    //获取status,判断请求是否成功
    val status = obj.getString("status")

    if(status.equals("1")){

      val regeocode = obj.getJSONObject("regeocode")

      if(regeocode!=null){

        val addressComponent = regeocode.getJSONObject("addressComponent")

        if(addressComponent!=null){

          val businessAreas = addressComponent.getJSONArray("businessAreas")

          if(businessAreas!=null && businessAreas.size()>0){

            val areas: util.List[BusinessArea] =
                        try{businessAreas.toJavaList(classOf[BusinessArea])}
                                              catch {case e:Exception=>{
                                                println(businessAreas.toJSONString)
                                                new util.ArrayList[BusinessArea]()
                                              }}

            import scala.collection.JavaConverters._

            result = areas.asScala.map(_.name).mkString(",")
          }
        }
      }

    }
    result
  }*/
}
