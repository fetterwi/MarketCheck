package tag

import `trait`.MakeTag
import org.apache.spark.sql.Row

/**
  * 生成渠道标签
  */
object ChannelTag extends MakeTag{
  /**
    * 生成标签，后续不同标签有不同逻辑
    *
    * @param args
    * @return
    */
  override def make(args: Any*): Map[String, Double] = {

    var result = Map[String,Double]()
    //1、取出数据
    val row = args.head.asInstanceOf[Row]
    //2、生成标签
    val channel = row.getAs[String]("channelid")

    result +=(s"CN_${channel}"->1.0)
    //3、数据返回

    result
  }
}
