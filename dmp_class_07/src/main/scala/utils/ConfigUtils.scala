package utils

import com.typesafe.config.ConfigFactory

/**
  * 配置文件读取类
  */
object ConfigUtils {
  //加载配置文件
  val conf = ConfigFactory.load()

  val SPARK_SQL_AUTOBROADCASTJOINTHRESHOLD = conf.getString("spark.sql.autoBroadcastJoinThreshold")

  val SPARK_SQL_SHUFFLE_PARTITIONS = conf.getString("spark.sql.shuffle.partitions")

  val SPARK_SHUFFLE_COMPRESS = conf.getString("spark.shuffle.compress")

  val SPARK_SHUFFLE_IO_MAXRETRIES = conf.getString("spark.shuffle.io.maxRetries")

  val SPARK_SHUFFLE_IO_RETRYWAIT = conf.getString("spark.shuffle.io.retryWait")

  val SPARK_BROADCAST_COMPRESS = conf.getString("spark.broadcast.compress")

  val SPARK_SERIALIZER = conf.getString("spark.serializer")

  val SPARK_MEMORY_FRACTION = conf.getString("spark.memory.fraction")

  val SPARK_MEMORY_STORAGEFRACTION = conf.getString("spark.memory.storageFraction")

  val SPARK_DEFAULT_PARALLELISM = conf.getString("spark.default.parallelism")

  val SPARK_LOCALITY_WAIT = conf.getString("spark.locality.wait")

  val SPARK_SPECULATION = conf.getString("spark.speculation.flag")

  val SPARK_SPECULATION_MULTIPLIER = conf.getString("spark.speculation.multiplier")

  //获取数据的路径
  val DATA_PATH = conf.getString("data.path")
  //获取纯真数据库名称
  val IP_FILE = conf.getString("IP_FILE")
  //获取纯真数据库目录
  val INSTALL_DIR = conf.getString("INSTALL_DIR")

  val GeoLiteCity = conf.getString("GeoLiteCity")
  //获取kudu master地址
  val KUDU_MASTER = conf.getString("kudu_master")
  //获取商圈的地址
  val URL = conf.getString("URL")
  //app字段文件路径
  val APPID_NAME = conf.getString("APPID_NAME")
  //设备字典路径
  val DEVICE_DIC = conf.getString("DEVICE_DIC")
  //标签衰减系数
  val ATTENU = conf.getString("attenu")

  //指定es节点
  val ES_NODES = conf.getString("es.nodes.list")
  //es节点端口
  val ES_PORT = conf.getString("es.port")
  //自动创建索引
  val ES_INDEX_AUTO_CREATE = conf.getString("es.index.auto.create")
  //自动发现器群节点
  val ES_NODES_DISCOVERY = conf.getString("es.nodes.discovery")
  val ES_NODES_WAN_ONLY = conf.getString("es.nodes.wan.only")
}
