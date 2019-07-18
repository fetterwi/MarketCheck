import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Random

case class Student(id:String,name:String,age:Int,classId:String)

case class ClassCID(id:String,name:String)

object DataTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.sql.autoBroadcastJoinThreshold","10485760")
      .appName("test").master("local[*]").getOrCreate()

    solution6(spark)

  }

  /**
    * 现象:多数task执行完毕,存在个别或者几个task一直在执行，从sparkUI界面应该可以看到这个task的数据量records
    *
    * 解决方案:
    *   1、过滤：一般应用于key为空的时候
    *   2、增加并行度
    *   3、局部聚合+全局聚合 ：聚合算子 group by
    *   4、将表广播出去  : 大表join小表
    *   5、将数据倾斜的key单独拿出来处理 : 大表 join 大表,其中一个大表分布比较均匀，另外一个不均匀的表产生倾斜的key不多
    *   6、随机前缀+ 扩容 : 大表 join 大表,其中一个大表分布比较均匀，另外一个不均匀的表产生倾斜的key比较多
    *   7、前面组合使用
    */

  /**
    * 1、过滤
    *  原因：如果大量数据join的key为空，那么这些key为空的数据就会聚集到一个分区中，从而导致数据倾斜
    *  解决方案：过滤key为空的数据
    *         如果key不为空导致的数据倾斜,但是这个key如果不影响最终计算结果，也可以过滤掉该key的数据
    * @param spark
    */
  def solution1(spark:SparkSession)={
    import spark.implicits._
    Seq[Student](
      Student("1","张三",18,""),
      Student("2","李四",19,""),
      Student("3","王五",29,""),
      Student("4","赵柳",21,""),
      Student("5","钱琪",22,""),
      Student("6","王霸",23,""),
      Student("7","李雷",24,""),
      Student("8","韩梅梅",25,"class_01"),
      Student("9","tom",26,"class_01"),
      Student("10","jerry",27,"class_01")
    ).toDF().filter("classId is not null and classId!=''").createOrReplaceTempView("student")

    Seq[ClassCID](
      ClassCID("class_01","大数据7期"),
      ClassCID("class_02","大数据8期"),
      ClassCID("class_03","大数据9期"),
      ClassCID("class_04","大数据10期"),
      ClassCID("class_05","python")
    ).toDF().createOrReplaceTempView("class_cid")

    spark.sql(
      """
        |select s.*,c.name from
        | student s left join class_cid c
        | on s.classId = c.id
      """.stripMargin).rdd.mapPartitionsWithIndex(mapPartitionWithIndexByStudent).collect()
  }

  def mapPartitionWithIndexByStudent(index:Int,it:Iterator[Row]):Iterator[Row]={
    println(s"index:${index},data:${it.toBuffer}")
    it
  }

  /**
    * 2、增加并行度
    * 原因：分区默认是hash分区：  分区号 = key.hashcode % 并行度,如果并行度设置的过小，那么就有可能产生多个key聚集到一个分区从而导致数据倾斜
    * 解决方案:增加并行度，
    *     spark.sql.shuffle.partitions 设置sparksql shuffle的并行度，默认并行度200，很多情况下200的并行度过小
    *
    * @param spark
    */
  def solution2(spark:SparkSession)={

  }

  /**
    * 局部聚合+全局聚合 : group by
    *
    * @param spark
    */
  def solution3(spark:SparkSession)={
    import spark.implicits._
    Seq[Student](
      Student("1", "张三", 18, "class_01"),
      Student("2", "李四", 19, "class_01"),
      Student("3", "王五", 29, "class_01"),
      Student("4", "赵柳", 21, "class_01"),
      Student("5", "钱琪", 22, "class_01"),
      Student("6", "王霸", 23, "class_01"),
      Student("7", "李雷", 24, "class_02"),
      Student("8", "韩梅梅", 25, "class_01"),
      Student("9", "tom", 26, "class_04"),
      Student("10", "jerry", 27, "class_05")
    ).toDF().createOrReplaceTempView("student")

    //StudentDF.selectExpr("classId").sample(false,0.5).collect().foreach(println)
    spark.udf.register("prefix",prefix _)
    spark.udf.register("unPrefix",unPrefix _)
    Seq[ClassCID](
      ClassCID("class_01","大数据7期"),
      ClassCID("class_02","大数据8期"),
      ClassCID("class_03","大数据9期"),
      ClassCID("class_04","大数据10期"),
      ClassCID("class_05","python")
    ).toDF().createOrReplaceTempView("class_cid")


    //局部聚合：对产生数据倾斜的key添加随机前缀后聚合
    spark.sql(
      """
        |select prefix(classId) as classId,count(1) as cn from
        | student group by prefix(classId)
      """.stripMargin).createOrReplaceTempView("t_tmp")
    //全局聚合:将局部聚合时添加的随机前缀去掉后再聚合
    spark.sql(
      """
        |select unPrefix(classId),sum(cn)
        | from t_tmp group by unPrefix(classId)
      """.stripMargin).show


  }

  def unPrefix(id:String):String={
    if(id.contains("@")){
      id.split("@")(1)
    }else{
      id
    }
  }

  def prefix(id:String):String={
    if(id.equals("class_01"))
    s"${Random.nextInt(10)}@${id}"
    else
      id
  }

  /**
    * 将表广播出去  : 适用于大表join小表
    * 解决方案:  1、设置自动广播 ：spark.sql.autoBroadcastJoinThreshold
    *            2、有时候可能设置了自动广播参数之后，小表并没有广播出去，这时候需要 spark.sql("cache table 小表")
    * @param spark
    */
  def solution4(spark:SparkSession)={
    import spark.implicits._
    Seq[Student](
      Student("1", "张三", 18, "class_01"),
      Student("2", "李四", 19, "class_01"),
      Student("3", "王五", 29, "class_01"),
      Student("4", "赵柳", 21, "class_01"),
      Student("5", "钱琪", 22, "class_01"),
      Student("6", "王霸", 23, "class_01"),
      Student("7", "李雷", 24, "class_02"),
      Student("8", "韩梅梅", 25, "class_01"),
      Student("9", "tom", 26, "class_04"),
      Student("10", "jerry", 27, "class_05")
    ).toDF().createOrReplaceTempView("student")

    Seq[ClassCID](
      ClassCID("class_01","大数据7期"),
      ClassCID("class_02","大数据8期"),
      ClassCID("class_03","大数据9期"),
      ClassCID("class_04","大数据10期"),
      ClassCID("class_05","python")
    ).toDF().createOrReplaceTempView("class_cid")

    spark.sql("cache table class_cid")

    spark.sql(
      """
        |select s.*,c.name
        |from student s left join class_cid c
        |on s.classId = c.id
      """.stripMargin).rdd.mapPartitionsWithIndex(mapPartitionWithIndexByStudent).collect()

  }

  /**
    * 5、将数据倾斜的key单独拿出来处理 : 大表 join 大表,其中一个大表分布比较均匀，另外一个不均匀的表产生倾斜的key不多
    *
    * @param spark
    */
  def solution5(spark:SparkSession)={
    import spark.implicits._
    val studentDF = Seq[Student](
      Student("1", "张三", 18, "class_01"),
      Student("2", "李四", 19, "class_01"),
      Student("3", "王五", 29, "class_01"),
      Student("4", "赵柳", 21, "class_01"),
      Student("5", "钱琪", 22, "class_01"),
      Student("6", "王霸", 23, "class_01"),
      Student("7", "李雷", 24, "class_02"),
      Student("8", "韩梅梅", 25, "class_01"),
      Student("9", "tom", 26, "class_04"),
      Student("10", "jerry", 27, "class_05")
    ).toDF()
    spark.udf.register("studentPrefix",studentPrefix _)
    spark.udf.register("classPrefix",classPrefix _)
    //class_01这个key发生数据倾斜
    studentDF.filter("classId='class_01'").selectExpr("studentPrefix(classId) as classId","id","name","age").createOrReplaceTempView("t_student_data")
    studentDF.filter("classId!='class_01'").createOrReplaceTempView("t_student_other")

    val classDF = Seq[ClassCID](
      ClassCID("class_01", "大数据7期"),
      ClassCID("class_02", "大数据8期"),
      ClassCID("class_03", "大数据9期"),
      ClassCID("class_04", "大数据10期"),
      ClassCID("class_05", "python")
    ).toDF()
    val classDataDF: Dataset[Row] = classDF.filter("id='class_01'")
    classDF.filter("id!='class_01'").createOrReplaceTempView("t_class_other")

    //没有发生数据倾斜的key正常处理
    spark.sql(
      """
        |select a.*,b.name
        | from t_student_other a left join t_class_other b
        | on a.classId = b.id
      """.stripMargin).createOrReplaceTempView("t_tmp_1")
    //发生数据倾斜的表对应的key加上随机前缀

    //扩容后的表
    extendTable(spark,classDataDF).createOrReplaceTempView("t_class_data")
    spark.sql("select * from t_class_data").show
    spark.sql(
      """
        |select a.id,a.name,a.classId,b.id,b.name
        | from t_student_data a left join t_class_data b
        | on a.classId = b.id
      """.stripMargin).createOrReplaceTempView("t_tmp_2")

    spark.sql(
      """
        |select * from t_tmp_2
      """.stripMargin).show


  }

  /**
    * 扩容表
    * @param classDataDF
    * @return
    */
  def extendTable(spark:SparkSession,classDataDF: Dataset[Row]):Dataset[Row]={
    var result:Dataset[Row] = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],classDataDF.schema)
    for (i<- 0 until(10)){
      result = result.union(classDataDF.selectExpr(s"classPrefix(id,${i}) as id","name"))
    }
    result
  }

  def classPrefix(id:String,i:Int):String={
    s"${i}@${id}"
  }
  /**
    * Student("1", "张三", 18, "1@class_01"),
    * Student("2", "李四", 19, "2@class_01"),
    * Student("3", "王五", 29, "4@class_01"),
    * Student("4", "赵柳", 21, "5@class_01"),
    * Student("5", "钱琪", 22, "6@class_01"),
    * Student("6", "王霸", 23, "7@class_01"),
    *
    *
    *
    * ClassCID("0@class_01", "大数据7期"),
    * ClassCID("1@class_01", "大数据7期"),
    * ClassCID("2@class_01", "大数据7期"),
    * ClassCID("3@class_01", "大数据7期"),
    * ClassCID("4@class_01", "大数据7期"),
    * ClassCID("5@class_01", "大数据7期"),
    * ClassCID("6@class_01", "大数据7期"),
    * ClassCID("7@class_01", "大数据7期"),
    * ClassCID("8@class_01", "大数据7期"),
    * ClassCID("9@class_01", "大数据7期"),
    *
    * @param classId
    * @return
    */
  def studentPrefix(classId:String):String={
    s"${Random.nextInt(10)}@${classId}"
  }

  /**
    * 6、随机前缀+ 扩容 : 大表 join 大表,其中一个大表分布比较均匀，另外一个不均匀的表产生倾斜的key比较多
    * @param sparkSession
    */
  def solution6(sparkSession: SparkSession)={
    import sparkSession.implicits._

    sparkSession.udf.register("studentPrefix",studentPrefix _)
    sparkSession.udf.register("classPrefix",classPrefix _)
    Seq[Student](
      Student("1", "张三", 18, "class_01"),
      Student("2", "李四", 19, "class_01"),
      Student("3", "王五", 29, "class_01"),
      Student("4", "赵柳", 21, "class_01"),
      Student("5", "钱琪", 22, "class_02"),
      Student("6", "王霸", 23, "class_02"),
      Student("7", "李雷", 24, "class_02"),
      Student("8", "韩梅梅", 25, "class_02"),
      Student("9", "tom", 26, "class_04"),
      Student("10", "jerry", 27, "class_05")
    ).toDF().selectExpr("studentPrefix(classId) as classId","id","name","age").createOrReplaceTempView("student")

    val classDF = Seq[ClassCID](
      ClassCID("class_01", "大数据7期"),
      ClassCID("class_02", "大数据8期"),
      ClassCID("class_03", "大数据9期"),
      ClassCID("class_04", "大数据10期"),
      ClassCID("class_05", "python")
    ).toDF()

    extendTable(sparkSession,classDF).createOrReplaceTempView("t_class")

    sparkSession.sql(
      """
        |select s.*,c.name
        | from student s left join t_class c
        | on s.classId = c.id
      """.stripMargin).show


  }


}
