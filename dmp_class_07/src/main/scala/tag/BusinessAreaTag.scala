package tag

import `trait`.MakeTag
import ch.hsr.geohash.GeoHash
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.JdbcUtils

/**
  * 生成商圈标签
  */
object BusinessAreaTag extends MakeTag{
  /**
    * 生成标签，后续不同标签有不同逻辑
    *
    * @param args
    * @return
    */
  override def make(args: Any*): Map[String, Double] = {

    var result = Map[String,Double]()
    //1、取出商圈信息
    val row = args.head.asInstanceOf[Row]
    //商圈信息
    val areas = row.getAs[String]("areas")
    //3、生成标签  商圈,商圈,商圈..
    if(StringUtils.isNotBlank(areas)){

      areas.split(",").foreach(area=>result+=(s"BA_${area}"->1.0))
    }
    //4、数据返回
    result
  }
  /*override def make(args: Any*): Map[String, Double] = {

    var result = Map[String,Double]()
    //1、取出经度、纬度
    val row = args.head.asInstanceOf[Row]
    //经度
    val longitude = row.getAs[Float]("longitude")
    //纬度
    val latitude = row.getAs[Float]("latitude")
    //2、根据经纬度生成编码从商圈表中查询对应的商圈信息
    val geoHashCode = GeoHash.geoHashStringWithCharacterPrecision(latitude.toDouble,longitude.toDouble,8)

    val areas: String = JdbcUtils.readBusinessArea(geoHashCode)
    //3、生成标签  商圈,商圈,商圈..
    areas.split(",").foreach(area=>result+=(s"BA_${area}"->1.0))
    //4、数据返回
    result
  }*/
}
