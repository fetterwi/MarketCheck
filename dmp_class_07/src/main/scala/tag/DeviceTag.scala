package tag

import `trait`.MakeTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * 生成设备标签
  */
object DeviceTag extends MakeTag{
  /**
    * 生成标签，后续不同标签有不同逻辑
    *
    * @param args
    * @return
    */
  override def make(args: Any*): Map[String, Double] = {

    var result= Map[String,Double]()
    //1、取出数据
    val row = args.head.asInstanceOf[Row]

    val bc = args.last.asInstanceOf[Broadcast[Map[String,String]]].value
    //2、将字段值转成字典值
    //设备型号、设备类型01、设备类型02、运营商、联网方式
    //设备型号
    val device = row.getAs[String]("device")
    //设备类型
    val client = row.getAs[Long]("client")

    val devicetype = row.getAs[Long]("devicetype")
    //运营商
    val ispname = row.getAs[String]("ispname")
    //联网方式
    val networkmannername = row.getAs[String]("networkmannername")
    //设备类型字典值
    val clientBc = bc(client.toString)
    //运营商字典值
    val ipsnameBc = bc(ispname)
    //联网方式字典值
    val networkmannernameBc = bc(networkmannername)
    //3、生成标签
    result += (s"CB_${clientBc}"->1.0)
    result += (s"ISP_${ipsnameBc}"->1.0)
    result += (s"NM_${networkmannernameBc}"->1.0)
    result += (s"DT_${devicetype.toString}"->1.0)
    result += (s"DEVICE_${device}"->1.0)
    //4、返回数据
    result
  }
}
