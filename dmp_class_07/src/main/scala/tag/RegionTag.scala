package tag

import `trait`.MakeTag
import org.apache.spark.sql.Row

/**
  * 生成地域标签
  */
object RegionTag extends MakeTag{
  /**
    * 生成标签，后续不同标签有不同逻辑
    *
    * @param args
    * @return
    */
  override def make(args: Any*): Map[String, Double] = {

    var result = Map[String,Double]()
    //1、获取数据
    val row = args.head.asInstanceOf[Row]
    //2、生成标签
    //省
    val proviceName = row.getAs[String]("proviceName")
    //城市
    val cityName = row.getAs[String]("city")

    result +=(s"PN_${proviceName}"->1.0)
    result +=(s"CTN_${cityName}"->1.0)
    //3、返回数据
    result
  }
}
