import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.SparkSession

object GraphTest {

  def main(args: Array[String]): Unit = {

    val spark= SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    //1、构建点 (id,（属性）)
    /**
      * 1 张三18
      * 2 李四19
      * 3 王五20
      * 4 赵六21
      * 5 韩梅梅22
      * 6 李雷23
      * 7 小明249tom2510jerry2611ession27
      */
    val vertices = spark.sparkContext.parallelize(Seq[(VertexId, (String, Int))](
      (1, ("张三", 18)),
      (2, ("李四", 19)),
      (3, ("王五", 20)),
      (4, ("赵六", 21)),
      (5, ("韩梅梅", 22)),
      (6, ("李雷", 23)),
      (7, ("小明", 24)),
      (9, ("tom", 25)),
      (10, ("jerry", 26)),
      (11, ("ession", 27))
    ))
    //2、构建边
    val edge = spark.sparkContext.parallelize(Seq[Edge[Int]](
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(3, 133, 0),
      Edge(4, 133, 0),
      Edge(5, 133, 0),
      Edge(4, 155, 0),
      Edge(5, 155, 0),
      Edge(6, 155, 0),
      Edge(7, 155, 0),
      Edge(9, 188, 0),
      Edge(10, 188, 0),
      Edge(11, 188, 0)
    ))

    //构建图
    val graph = Graph(vertices,edge)
    //查看图中所有点
    //graph.vertices.foreach(println)
    //查看图中所有边
    //graph.edges.foreach(println)
    //println("======点========>"+graph.numVertices)
    //println("======边========>"+graph.numEdges)
    //构建连通图
    val components = graph.connectedComponents()
    //[1,2,3,4,5,6,7]  [9,10,11]
    //components.vertices.map(x=>(x._2,x._1)).groupByKey().foreach
    //连通图点 (id,aggid)
    val vers = components.vertices
    //(id,aggid) join (id,(name,age)) => (id,(aggid,(name,age)))
    //[(1,zhangsan,18),(2,lisi,19)...]  [(9,name,age),(10,name,age),(11,name,age)]
    //(id,com((id,name,age),()))
    vers.join(vertices).map {
      case (id,(aggid,(name,age)))=>
        (aggid,(id,name,age))
    }.groupByKey().collect()

  }
}
