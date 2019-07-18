package `trait`

trait MakeTag {
  /**
    * 生成标签，后续不同标签有不同逻辑
    * @param args
    * @return
    */
  def make(args:Any*):Map[String,Double]
}
