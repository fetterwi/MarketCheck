import es.EsProcess
import etl.ParseIp
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import pro._
import utils.ConfigUtils

object Application {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
        .appName("app")
        .master("local[*]")
        .config("spark.sql.autoBroadcastJoinThreshold",ConfigUtils.SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD)
        .config("spark.sql.shuffle.partitions",ConfigUtils.SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.shuffle.compress",ConfigUtils.SPARK_SHUFFLE_COMPRESS)
        .config("spark.shuffle.io.maxRetries",ConfigUtils.SPARK_SHUFFLE_IO_MAXRETRIES)
        .config("spark.shuffle.io.retryWait",ConfigUtils.SPARK_SHUFFLE_IO_RETRYWAIT)
        .config("spark.broadcast.compress",ConfigUtils.SPARK_BROADCAST_COMPRESS)
        .config("spark.serializer",ConfigUtils.SPARK_SERIALIZER)
        .config("spark.memory.fraction",ConfigUtils.SPARK_MEMORY_FRACTION)
        .config("spark.memory.storageFraction",ConfigUtils.SPARK_MEMORY_STORAGEFRACTION)
        .config("spark.default.parallelism",ConfigUtils.SPARK_DEFAULT_PARALLELISM)
        .config("spark.locality.wait",ConfigUtils.SPARK_LOCALITY_WAIT)
        .config("spark.speculation.multiplier",ConfigUtils.SPARK_SPECULATION_MULTIPLIER)
        .config("spark.speculation.flag",ConfigUtils.SPARK_SERIALIZER)
        .config("es.nodes",ConfigUtils.ES_NODES)
      .config("es.port",ConfigUtils.ES_PORT)
      .config("es.index.auto.create",ConfigUtils.ES_INDEX_AUTO_CREATE)
      .config("es.nodes.discovery",ConfigUtils.ES_NODES_DISCOVERY)
      .config("es.nodes.wan.only",ConfigUtils.ES_NODES_WAN_ONLY)
        .getOrCreate()


    val context = new KuduContext(ConfigUtils.KUDU_MASTER,spark.sparkContext)
    //TODO 解析IP获取经纬度、省市
    //ParseIp.process(spark,context)
    //TODO 统计各省市的地域分布情况
    //ProviceCityAnalysis.process(spark,context)
    //TODO 统计广告投放的地域分布情况
    //AdRegionAnalysis.process(spark,context)
    //TODO 统计广告投放的APP分布情况
    //AppAnaylysis.process(spark,context)
    //TODO 统计广告投放设备类型分布情况
    //AdDeviceTypeAnalysis.process(spark,context)
    //TODO 统计广告投放的网络设备分布情况
    //AdNetworkAnalysis.process(spark,context)
    //TODO 统计广告投放运营商分布情况
    //AdOperatorAnalysis.process(spark,context)
    //TODO 统计广告投放渠道分布情况
    //AdChannelAnalysis.process(spark,context)
    //TODO 生成商圈库
    //BusinessAreaProcess.process(spark,context)
    //TODO 数据标签化
    //TagProcess.process(spark,context)
    //TODO 数据落地ES
    EsProcess.process(spark,context)

  }
}
