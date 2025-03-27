package sessionapp

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window

object Main {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSessionFactory.get("EcommerceSessionApp")

    import spark.implicits._

    val rawDF = spark.read
      .option("header", "true")
      .parquet("data/cached/raw_parquet/")
      .withColumn("event_time_kst", F.from_utc_timestamp($"event_time", "Asia/Seoul"))

    rawDF.show(5, truncate = false)
    rawDF.printSchema()

    val windowSpec = Window.partitionBy("user_id").orderBy("event_time_kst")

    val withLag = rawDF
      .withColumn("prev_event_time", F.lag("event_time_kst", 1).over(windowSpec))
      .withColumn("diff_sec", F.unix_timestamp($"event_time_kst") - F.unix_timestamp($"prev_event_time"))
      .withColumn("new_session", F.when($"diff_sec".isNull || $"diff_sec" > 300, 1).otherwise(0))

    withLag.select(
      $"user_id", $"event_time_kst", $"prev_event_time", $"diff_sec", $"new_session"
    ).show(10, truncate = false)

    val withSessionId = withLag
      .withColumn("session_index", F.sum("new_session").over(windowSpec.rowsBetween(Window.unboundedPreceding, 0)))
      .withColumn("session_id", F.concat_ws("_", $"user_id", $"session_index"))

    withSessionId.select(
      $"user_id", $"event_time_kst", $"session_index", $"session_id"
    ).show(10, truncate = false)

    val finalDF = withSessionId
      .withColumn("event_date", F.to_date($"event_time_kst"))
      .select(
        $"user_id", $"event_time_kst".alias("event_time"), $"event_type", $"product_id",
        $"category_id", $"category_code", $"brand", $"price",
        $"session_id", $"event_date"
      )

    finalDF.printSchema()
    finalDF.show(10, truncate = false)

    val outputPath = "output/ecommerce_activity"

    finalDF.write
      .mode("overwrite")
      .partitionBy("event_date")
      .option("compression", "snappy")
      .parquet(outputPath)

    println(s"저장 완료: $outputPath")

    spark.stop()
  }
}
