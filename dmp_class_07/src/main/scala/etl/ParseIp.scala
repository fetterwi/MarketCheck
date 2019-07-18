package etl

import `trait`.Process
import com.maxmind.geoip.{Location, LookupService}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import utils._

/**
  * 解析ip,生成经纬度、省市
  */
object ParseIp extends Process{
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */

  //定义数据保存表ODS
  val SINK_TABLE = s"ODS_${DateUtils.getNow()}"
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、获取数据
    spark.read.json(ConfigUtils.DATA_PATH).createOrReplaceTempView("t_data_info")
    //2、获取所有的ip,过滤掉为空的ip
    val ipDF = spark.sql("select ip from t_data_info where ip is not null and ip!=''")
    val ips = ipDF.distinct().rdd

    import spark.implicits._
    //3、解析经纬度、省市
    ips.map(row=>{
      //获取ip
      val ip = row.getAs[String]("ip")

      //解析经纬度
      val service: LookupService = new LookupService(ConfigUtils.GeoLiteCity)
      //ip所处位置
      val location: Location = service.getLocation(ip)
      //经度
      val longitude: Float = location.longitude
      //纬度
      val latitude: Float = location.latitude
      //获取ip地址
      val address = new IPAddressUtils()

      val region: IPLocation = address.getregion(ip)
      //省份
      val proviceName: String = region.getRegion
      //城市
      val city: String = region.getCity

      (ip,longitude,latitude,proviceName,city)
    }).toDF("ip","longitude","latitude","proviceName","city")
      .createOrReplaceTempView("t_ip_info")
    //4、补充原始数据
    val result = spark.sql(
      """
        |select
        | a.ip,
        |a.sessionid,
        |a.advertisersid,
        |a.adorderid,
        |a.adcreativeid,
        |a.adplatformproviderid,
        |a.sdkversion,
        |a.adplatformkey,
        |a.putinmodeltype,
        |a.requestmode,
        |a.adprice,
        |a.adppprice,
        |a.requestdate,
        |a.appid,
        |a.appname,
        |a.uuid,
        |a.device,
        |a.client,
        |a.osversion,
        |a.density,
        |a.pw,
        |a.ph,
        |b.longitude,
        |b.latitude,
        |b.proviceName,
        |b.city,
        |a.ispid,
        |a.ispname,
        |a.networkmannerid,
        |a.networkmannername,
        |a.iseffective,
        |a.isbilling,
        |a.adspacetype,
        |a.adspacetypename,
        |a.devicetype,
        |a.processnode,
        |a.apptype,
        |a.district,
        |a.paymode,
        |a.isbid,
        |a.bidprice,
        |a.winprice,
        |a.iswin,
        |a.cur,
        |a.rate,
        |a.cnywinprice,
        |a.imei,
        |a.mac,
        |a.idfa,
        |a.openudid,
        |a.androidid,
        |a.rtbprovince,
        |a.rtbcity,
        |a.rtbdistrict,
        |a.rtbstreet,
        |a.storeurl,
        |a.realip,
        |a.isqualityapp,
        |a.bidfloor,
        |a.aw,
        |a.ah,
        |a.imeimd5,
        |a.macmd5,
        |a.idfamd5,
        |a.openudidmd5,
        |a.androididmd5,
        |a.imeisha1,
        |a.macsha1,
        |a.idfasha1,
        |a.openudidsha1,
        |a.androididsha1,
        |a.uuidunknow,
        |a.userid,
        |a.iptype,
        |a.initbidprice,
        |a.adpayment,
        |a.agentrate,
        |a.lomarkrate,
        |a.adxrate,
        |a.title,
        |a.keywords,
        |a.tagid,
        |a.callbackdate,
        |a.channelid,
        |a.mediatype,
        |a.email,
        |a.tel,
        |a.sex,
        |a.age
        | from t_data_info a left join t_ip_info b
        | on a.ip = b.ip
      """.stripMargin)
    //5、数据写入ods
    val schema = result.schema
    //设置表的属性
    val options = new CreateTableOptions()
    //设置表的分区策略 分区字段 分区个数  副本数
    val columns = Seq[String]("ip")
    import scala.collection.JavaConverters._
    options.addHashPartitions(columns.asJava,3)

    options.setNumReplicas(3)

    //设置主键
    val keys = columns
    KuduUtils.writeToKudu(kuduContext,schema,options,SINK_TABLE,result,keys)
  }
}
