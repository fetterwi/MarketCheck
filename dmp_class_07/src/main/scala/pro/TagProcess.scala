package pro

import java.sql.DriverManager

import `trait`.Process
import agg.TagAgg
import ch.hsr.geohash.GeoHash
import graph.UserGraph
import merge.TagMerge
import org.apache.commons.lang3.StringUtils
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import tag._
import utils.{ConfigUtils, DateUtils, KuduUtils}

/**
  * 数据标签化
  */
object TagProcess extends Process{

  //指定数据原表
  val SOURCE_TABLE = s"ODS_${DateUtils.getNow()}"
  //指定kudu集群的信息
  val options = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )

  //指定商圈表
  val BUSINESS_AREA_TABLE = s"business_area_${DateUtils.getNow()}"
  //指定kudu集群商圈信息
  val areaOptions = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->BUSINESS_AREA_TABLE
  )
  //历史表
  val HISTORY_TABLE = s"TAG_${DateUtils.getYesterDay()}"

  val historyOptions = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->HISTORY_TABLE
  )

  //标签存储表
  val SINK_TABLE = s"TAG_${DateUtils.getNow()}"
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    *
    * 标签:
    *   app、设备标签(设备型号、设备类型01、设备类型02、运营商、联网方式)、地域标签(省份、城市)、广告位类型、
    *   关键字、渠道、性别、年龄
    * 用户标识
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取ODS表的数据
    import spark.implicits._
    import org.apache.kudu.spark.kudu._
    val odsDF = spark.read.options(options).kudu
    //读取商圈信息
    spark.read.options(areaOptions).kudu.createOrReplaceTempView("t_business_area")
    //2、过滤不符合规范的数据，去重
    //select * from ods where (imei is not null and imei!='') or (mac is not null and mac!='') or
    val filterDF = odsDF.filter(
      """
        |(imei is not null and imei!='') or
        |(mac is not null and mac!='') or
        |(idfa is not null and idfa!='') or
        |(openudid is not null and openudid!='') or
        |(androidid is not null and androidid!='')
      """.stripMargin)

    filterDF.createOrReplaceTempView("t_ods")
    //3、app字典文件广播
    /**
      * 将商圈信息补充到ods中
      */
    spark.udf.register("getGeoHashCode",getGeoHashCode _)

    spark.sql("cache table t_business_area")
    val odsAreaDF = spark.sql(
              """
              select a.*,b.areas
               from t_ods a left join t_business_area b
               on getGeoHashCode(a.longitude,a.latitude) = b.genhash
                """.stripMargin)
    //看执行计划
    //odsAreaDF.explain()

    val appIDName = spark.read.textFile(ConfigUtils.APPID_NAME)
    //将字典文件数据收集到driver端，用于广播
    val appIdNameCollect: Array[(String, String)] = appIDName.map(line => {
      val arr = line.split("##")
      (arr(0), arr(1))
    }).collect()
    val appBc = spark.sparkContext.broadcast(appIdNameCollect.toMap)
    //4、设备字典文件广播
    val device = spark.read.textFile(ConfigUtils.DEVICE_DIC)
    //将字典文件数据收集到driver端，用于广播
    val deviceCollect = device.map(line => {
      val arr = line.split("##")
      (arr(0), arr(1))
    }).collect()
    val deviceBc:Broadcast[Map[String, String]] =  spark.sparkContext.broadcast(deviceCollect.toMap)


    //5、遍历数据生成用户标签
    val currentDayRdd: RDD[(String, (List[String], Map[String, Double]))] = odsAreaDF.rdd.map(row => {
      //1、生成app标签
      val appTag: Map[String, Double] = AppTag.make(row, appBc)

      //2、设备标签
      val deviceTag: Map[String, Double] = DeviceTag.make(row, deviceBc)

      //3、地域标签
      val reionTag = RegionTag.make(row)

      //4、广告位类型
      val adTypeTag = AdTypeTag.make(row)

      //5、关键字
      val keywordTags = KeywordTag.make(row)

      //6、渠道标签
      val channelTag = ChannelTag.make(row)

      //7、年龄标签
      val ageTag = AgeTag.make(row)

      //8、性别标签
      val sexTag = SexTag.make(row)

      //9、商圈标签
      val areaTag = BusinessAreaTag.make(row)
      //10、用户所有标识
      val userIds = getAllUserIds(row)
      //用户唯一标识
      val userId = userIds.head
      //用户所有标签
      val tags = appTag ++ deviceTag ++ reionTag ++ adTypeTag ++ keywordTags ++ channelTag ++ ageTag ++ sexTag ++ areaTag

      (userId, (userIds, tags))
    })
    //用户统一识别
    val graphRdd: RDD[(VertexId, (VertexId, (List[String], Map[String, Double])))] = UserGraph.graph(currentDayRdd)
    //标签聚合
    val currentDayTags: RDD[(VertexId, (List[String], Map[String, Double]))] = TagAgg.agg(graphRdd)
    //当天标签
    val currentTags: RDD[(String, (List[String], Map[String, Double]))] = currentDayTags.map {
      case (aggid, (allUserIds, tags)) =>
        (allUserIds.head, (allUserIds, tags))
    }

    //6、历史数据与当天数据合并
    //读取标签的历史数据
    val historyDF = spark.read.options(historyOptions).kudu

    val historyTag: RDD[(String, (List[String], Map[String, Double]))] = historyDF.rdd.map(row => {
      val userId = row.getAs[String]("userId")

      val allUserIdStr = row.getAs[String]("allUserId")

      val allUserIds: List[String] = allUserIdStr.split(",").toList
      //(BA_永昌,1.0),(BA_永昌,1.0)
      val tagStr = row.getAs[String]("tags")
      //val tags: Map[String, Double]
      //BA_永昌,1.0),(BA_永昌,1.0
      val subStringTag = tagStr.substring(1, tagStr.length - 1)
      //[BA_永昌,1.0 , BA_永昌,1.0]
      val tagArray: Array[String] = subStringTag.split("\\),\\(")

      val tags: Map[String, Double] = tagArray.map(str => {
        val arr = str.split(",")
        val tagName = arr(0)
        val attr = arr(1).toDouble
        (tagName, attr)
      }).toMap

      (userId, (allUserIds, tags))

    })

    //合并历史数据与当天数据
    val allTags = TagMerge.merge(historyTag,currentTags)

    //造一份历史数据
    val result = allTags.map {
      case (userId, (allUserIds, tags)) =>
        (userId, allUserIds.mkString(","), tags.toList.mkString(","))
    }.toDF("userId", "allUserId", "tags")

    //指定tag表的schema
    val schema = result.schema
    //指定分区策略 分区字段 分区数
    val opt = new CreateTableOptions()
    //分区字段
    val columns = Seq[String]("userId")
    import scala.collection.JavaConverters._
    opt.addHashPartitions(columns.asJava,3)
    //指定副本
    opt.setNumReplicas(3)
    //指定主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,opt,SINK_TABLE,result,keys)


  }

  /**
    * 获取geohashcode
    * @param longitude
    * @param latitude
    * @return
    */
  def getGeoHashCode(longitude:Float,latitude:Float):String={

    GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)
  }
  /**
    * 获取用户所有标识
    * @param row
    * @return
    */
  def getAllUserIds(row:Row):List[String]={

    var result = List[String]()

    val imei = row.getAs[String]("imei")

    val mac = row.getAs[String]("mac")

    val idfa = row.getAs[String]("idfa")

    val openudid = row.getAs[String]("openudid")

    val androidid = row.getAs[String]("androidid")

    if(StringUtils.isNotBlank(imei)){
      result = result.:+(imei)
    }

    if(StringUtils.isNotBlank(mac)){
      result = result.:+(mac)
    }

    if(StringUtils.isNotBlank(idfa)){
      result = result.:+(idfa)
    }

    if(StringUtils.isNotBlank(openudid)){
      result = result.:+(openudid)
    }

    if(StringUtils.isNotBlank(androidid)){
      result = result.:+(androidid)
    }
    result
  }
}
