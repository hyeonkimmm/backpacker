package sessionapp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CacheParquet {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionFactory.get("CacheCsvToParquet")

    val schema = StructType(Seq(
      StructField("event_time", TimestampType, true),
      StructField("event_type", StringType, true),
      StructField("product_id", IntegerType, true),
      StructField("category_id", LongType, true),
      StructField("category_code", StringType, true),
      StructField("brand", StringType, true),
      StructField("price", DoubleType, true),
      StructField("user_id", IntegerType, true),
      StructField("user_session", StringType, true)
    ))

    val inputPaths = Seq("data/2019-Oct.csv", "data/2019-Nov.csv")

    val df = spark.read
      .option("header", "true")
      .schema(schema)
      .csv(inputPaths: _*)

    val outputPath = "data/cached/raw_parquet"

    df.write
      .mode("overwrite")
      .parquet(outputPath)

    println(s"Cached to Parquet at: $outputPath")

    spark.stop()
  }
}
