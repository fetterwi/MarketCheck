package bean

import scala.collection.mutable.ListBuffer

class EsBean {

  /**
    * app、设备标签(设备型号、设备类型01、设备类型02、运营商、联网方式)、地域标签(省份、城市)、广告位类型、
    *   关键字、渠道、性别、年龄
    */
    //存放app标签
  private val app:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放设备型号标签
  private val deviceModel:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放设备类型标签
  private val client:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()

  private val deviceType:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放运营商标签
  private val operator:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放联网方式标签
  private val network:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放省标签
  private val provice:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //存放城市标签
  private val city:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //广告位类型标签
  private val adType:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //关键字标签
  private val keyword:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //渠道标签
  private val channel:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //性别标签
  private val sex:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //年龄标签
  private val age:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()
  //商圈标签
  private val businessArea:ListBuffer[(String,Double)] = new ListBuffer[(String, Double)]()

  def setApp(tag:(String,Double))={
    this.app.append(tag)
  }

  def setDeviceModel(tag:(String,Double))={
    this.deviceModel.append(tag)
  }

  def setClient(tag:(String,Double))={
    this.client.append(tag)
  }

  def setDeviceType(tag:(String,Double))={
    this.deviceType.append(tag)
  }

  def setOperator(tag:(String,Double))={
    this.operator.append(tag)
  }

  def setNetwork(tag:(String,Double))={
    this.network.append(tag)
  }

  def setProvice(tag:(String,Double))={
    this.provice.append(tag)
  }

  def setCity(tag:(String,Double))={
    this.city.append(tag)
  }
  def setAdType(tag:(String,Double))={
    this.adType.append(tag)
  }

  def setKeyword(tag:(String,Double))={
    this.keyword.append(tag)
  }

  def setChannel(tag:(String,Double))={
    this.channel.append(tag)
  }

  def setSex(tag:(String,Double))={
    this.sex.append(tag)
  }

  def setAge(tag:(String,Double))={
    this.age.append(tag)
  }

  def setBusinessArea(tag:(String,Double))={
    this.businessArea.append(tag)
  }

  def toMap():Map[String,String]={
   Map[String,String](
     "app"->this.app.toString(),
     "deviceModel"->this.deviceModel.toString(),
     "deviceType"->this.deviceType.toString(),
     "client"->this.client.toString(),
     "sex"->this.sex.toString(),
     "age"->this.age.toString(),
     "businessArea"->this.businessArea.toString(),
     "operator"->this.operator.toString(),
     "keyword"->this.keyword.toString(),
     "adType"->this.adType.toString(),
     "city"->this.city.toString(),
     "provice"->this.provice.toString(),
     "network"->this.network.toString(),
     "channel"->this.channel.toString()
   )
  }
}
