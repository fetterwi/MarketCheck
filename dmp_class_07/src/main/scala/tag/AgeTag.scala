package tag

import `trait`.MakeTag
import org.apache.spark.sql.Row

/**
  * 生成年龄标签
  */
object AgeTag extends MakeTag{
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
    val age = row.getAs[String]("age")

    result +=(s"AGE_${age}"->1.0)
    //3、数据返回
    result
  }
}
