package es

import `trait`.Process
import bean.EsBean
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.{ConfigUtils, DateUtils}

/**
  * 数据落地ES
  */
object EsProcess extends Process{
  //定义数据原表-标签表
  val SOURCE_TABLE = s"TAG_${DateUtils.getNow()}"
  //定义kudu信息
  val options = Map[String,String](
    "kudu.master"->ConfigUtils.KUDU_MASTER,
    "kudu.table"->SOURCE_TABLE
  )
  /**
    * 逻辑处理部分，后期不同的操作写不同的逻辑
    */
  override def process(spark: SparkSession, kuduContext: KuduContext): Unit = {
    //1、读取标签数据
    import org.apache.kudu.spark.kudu._
    val tagDF = spark.read.options(options).kudu
    //2、根据标签的前缀将标签分类
    //数据解析
    val tags = tagDF.rdd.map(row => {
      val userId = row.getAs[String]("userId")
      //获取用户所有标识字符串==>id1,id2,id3...
      val allUserIdStr = row.getAs[String]("allUserId")

      val allUserIdArr: Array[String] = allUserIdStr.split(",")

      val allUserIds = allUserIdArr.toList
      //获取用户所有标签字符串=>(标签名,权重),(标签名,权重)....
      val tagStr = row.getAs[String]("tags")
      //标签名,权重),(标签名,权重
      val tagSubStr = tagStr.substring(1, tagStr.length - 1)
      //[标签名,权重 , 标签名,权重]
      val tagArr: Array[String] = tagSubStr.split("\\),\\(")
      //用户所有标签
      val tags: Array[(String, Double)] = tagArr.map(str => {
        val arr = str.split(",")
        val tagName = arr(0)
        val attr = arr(1).toDouble
        (tagName, attr)
      })

      val tagMap = tags.toMap

      (userId, (allUserIds, tagMap))

    })
    //标签分类
    val result: RDD[(String, Map[String, String])] = tags.map {
      case (userId, (allUserIds, tagMap)) => {
        val esBean = new EsBean()

        tagMap.foreach {
          case (tagName, attr) if (tagName.startsWith("AT_")) => esBean.setAdType((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.startsWith("AGE_")) => esBean.setAge((tagName.substring(4), attr))
          case (tagName, attr) if (tagName.startsWith("APP_")) => esBean.setApp((tagName.substring(4), attr))
          case (tagName, attr) if (tagName.startsWith("BA_")) => esBean.setBusinessArea((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.startsWith("CN_")) => esBean.setChannel((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.contains("D00010001")) => esBean.setClient(("android", attr))
          case (tagName, attr) if (tagName.contains("D00010002")) => esBean.setClient(("ios", attr))
          case (tagName, attr) if (tagName.contains("D00010003")) => esBean.setClient(("wp", attr))
          case (tagName, attr) if (tagName.contains("D00010004")) => esBean.setClient(("other", attr))
          case (tagName, attr) if (tagName.contains("D00020001")) => esBean.setNetwork(("WIFI", attr))
          case (tagName, attr) if (tagName.contains("D00020002")) => esBean.setNetwork(("4G", attr))
          case (tagName, attr) if (tagName.contains("D00020003")) => esBean.setNetwork(("3G", attr))
          case (tagName, attr) if (tagName.contains("D00020004")) => esBean.setNetwork(("2G", attr))
          case (tagName, attr) if (tagName.contains("D00020005")) => esBean.setNetwork(("NETWORKOTHER", attr))
          case (tagName, attr) if (tagName.contains("D00030001")) => esBean.setOperator(("移动", attr))
          case (tagName, attr) if (tagName.contains("D00030002")) => esBean.setOperator(("联通", attr))
          case (tagName, attr) if (tagName.contains("D00030003")) => esBean.setOperator(("电信", attr))
          case (tagName, attr) if (tagName.contains("D00030004")) => esBean.setOperator(("OPERATOROTHER", attr))
          case (tagName, attr) if (tagName.startsWith("DT_")) => esBean.setDeviceType((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.startsWith("DEVICE_")) => esBean.setDeviceModel((tagName.substring(7), attr))
          case (tagName, attr) if (tagName.startsWith("KW_")) => esBean.setKeyword((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.startsWith("PN_")) => esBean.setProvice((tagName.substring(3), attr))
          case (tagName, attr) if (tagName.startsWith("CTN_")) => esBean.setCity((tagName.substring(4), attr))
          case (tagName, attr) if (tagName.startsWith("SEX_")) => esBean.setSex((tagName.substring(4), attr))
        }

        (userId, esBean.toMap())
      }

    }

    //3、将数据落地ES
    import org.elasticsearch.spark._
    result.saveToEsWithMeta("tagIndex/doc")
  }
}
