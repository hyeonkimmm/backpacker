package sessionapp

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def get(appName: String, master: Option[String] = Some("local[*]")): SparkSession = {
    val builder = SparkSession.builder()
      .appName(appName)
      .config("spark.sql.shuffle.partitions", "32")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()

    master.foreach(builder.master)

    builder.getOrCreate()
  }
}
