package utils

import org.apache.kudu.Schema
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
  * kudu写入帮助类
  */
object KuduUtils {

  def writeToKudu(context:KuduContext, schema:StructType, options:CreateTableOptions,
                  tableName:String, data:DataFrame,keys:Seq[String])={

    //如果表不存在则创建表
    if(!context.tableExists(tableName)){

      context.createTable(tableName,schema,keys,options)
    }

    context.insertRows(data,tableName)
  }
}
