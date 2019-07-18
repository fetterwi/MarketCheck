package tag

import `trait`.MakeTag
import org.apache.spark.sql.Row

/**
  * 生成关键字标签
  */
object KeywordTag extends MakeTag{
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

    val keywords = row.getAs[String]("keywords")

    keywords.split(",").foreach(kw=>{
      result +=(s"KW_${kw}"->1.0)
    })
    //3、数据返回
    result
  }
}
