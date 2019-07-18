package tag

import `trait`.MakeTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成app标签
  */
object AppTag extends MakeTag{
  /**
    * 生成标签，后续不同标签有不同逻辑
    *
    * @param args
    * @return
    */
  override def make(args: Any*): Map[String, Double] = {

    var result = Map[String,Double]()
    //1、获取数据
    //row对象
    val row = args.head.asInstanceOf[Row]
    //字典文件夹
    val bc = args.last.asInstanceOf[Broadcast[Map[String, String]]]
    //2、判断如果name为空，从字典文件中获取name
    var appName:String = row.getAs[String]("appname")
    val appId = row.getAs[String]("appid")

    appName = Option(appName) match {
      case Some(appName) =>
        appName
      case None =>
        bc.value(appId)
    }
    //美图  都挺好
    //app:美图、王者荣耀    kw:都挺好、吃鸡
    //3、打标签
    result+=(s"APP_${appName}"->1.0)
    //4、数据返回

    result
  }
}
