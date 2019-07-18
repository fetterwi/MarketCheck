package utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object JdbcUtils {
  /**
    * 通过geohash获取商圈列表
    * @param genHashCode
    * @return
    */
  def readBusinessArea(genHashCode:String):String={
    //1、加载驱动
    Class.forName("com.cloudera.impala.jdbc41.Driver")
    var areas = ""
    var connection:Connection = null
    var statement:PreparedStatement = null
    //2、获取连接
    try{

      connection = DriverManager.getConnection("jdbc:impala://192.168.188.100:21050/default")
      //3、创建statement对象
      val sql = s"select areas from business_area_${DateUtils.getNow()} where genhash=?"
      statement = connection.prepareStatement(sql)
      //4、查询
      statement.setString(1,genHashCode)
      val resultSet: ResultSet = statement.executeQuery()

      while (resultSet.next()){
        areas = resultSet.getString("areas")
      }
    }finally {
      if(statement!=null)
        statement.close()
      if(connection!=null)
        connection.close()
    }
    //5、数据返回
    areas
  }
}
