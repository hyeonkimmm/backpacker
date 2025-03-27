package sessionapp

import org.apache.spark.sql.{SparkSession, functions => F}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField

object WAUCalculator {
  def main(args: Array[String]): Unit = {
    val inputDate = if (args.length >= 1) args(0) else "2019-10-10"
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val localDate = LocalDate.parse(inputDate, formatter)

    val dayOfWeek = localDate.get(ChronoField.DAY_OF_WEEK)
    val weekStartDate = localDate.minusDays(dayOfWeek - 1)
    val weekStartStr = weekStartDate.format(formatter)
    val weekEndStr = weekStartDate.plusDays(6).format(formatter)

    println(s"기준 주: $weekStartStr ~ $weekEndStr")

    val spark: SparkSession = SparkSessionFactory.get("WAUCalculator")
    import spark.implicits._

    val df = spark.read
      .option("basePath", "output/ecommerce_activity")
      .parquet("output/ecommerce_activity")
      .withColumn("event_date", F.col("event_date").cast("date"))

    df.createOrReplaceTempView("ecommerce_events")

    val wauByUser = spark.sql(
      s"""
          SELECT
            DATE('$weekStartStr') AS week_start_date,
            COUNT(DISTINCT user_id) AS wau_user
          FROM ecommerce_events
          WHERE event_date BETWEEN DATE('$weekStartStr') AND DATE('$weekEndStr')
        """)

    println("\nWAU (by user_id):")
    wauByUser.show(truncate = false)

    val wauBySession = spark.sql(
      s"""
          SELECT
            DATE('$weekStartStr') AS week_start_date,
            COUNT(DISTINCT session_id) AS wau_session
          FROM ecommerce_events
          WHERE event_date BETWEEN DATE('$weekStartStr') AND DATE('$weekEndStr')
        """)

    println("\nWAU (by session_id):")
    wauBySession.show(truncate = false)

    spark.stop()
  }
}
