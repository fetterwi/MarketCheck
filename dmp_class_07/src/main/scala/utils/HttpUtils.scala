package utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod

/**
  * http帮助类
  */
object HttpUtils {

  /**
    * 发起get请求
    * @return
    */
  def get(url:String):String = {

    val client = new HttpClient()

    val getMethod = new GetMethod(url)

    val code: Int = client.executeMethod(getMethod)

    if(code==200){
      getMethod.getResponseBodyAsString
    }else{
      ""
    }
  }
}
