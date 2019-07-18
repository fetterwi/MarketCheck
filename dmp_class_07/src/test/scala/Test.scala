import org.apache.spark.sql.SparkSession
import utils.{ConfigUtils, DateUtils}

object Test{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("test")
      .master("local[*]")
      .getOrCreate()

    import org.apache.kudu.spark.kudu._
    spark.read.option("kudu.master",ConfigUtils.KUDU_MASTER)
      .option("kudu.table",s"TAG_${DateUtils.getNow()}")
      .kudu
     .show()

  }
}
