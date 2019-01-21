package ar.edu.itba.seminario

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}




object DolarETLProcessor{

  def main(args: Array[String]): Unit = {

    val pricesTableName = "BCRA_PRICES"
    val pricesOficialTableName = "BCRA_PRICES_OFICIAL"
    val eventsTableName = "BCRA_EVENTS"


    val spark = SparkSession.
      builder.
      appName("BCRA-Dolar:ETL").
      getOrCreate()

    val prices = PostgresUtils.readTable(spark, pricesTableName)
      .withColumnRenamed("fecha", "date_price")
      .withColumnRenamed("precio", "price")
      .withColumn("year", year(col("date_price")))
      .withColumn("month", functions.month(col("date_price")))
      .withColumn("day", functions.dayofmonth(col("date_price")))

    val official = PostgresUtils.readTable(spark, pricesOficialTableName).
      withColumnRenamed("precio", "price_official")
    val event = PostgresUtils.readTable(spark, eventsTableName)
      .withColumnRenamed("fecha", "event_date")
      .withColumnRenamed("evento", "event")
      .withColumnRenamed("tipo", "type")


    val joinPrices = prices.sort().join(official, official("fecha") === prices("date_price"), "left_outer")
      .drop("fecha")

    val joinPricesEvents = joinPrices.sort(joinPrices("date_price").desc).join(event.sort(event("event_date")),
      joinPrices("date_price") === event("event_date"), "left_outer")
      .drop("event_date")


    val bigdata = joinPrices.withColumn("date_price", joinPricesEvents("date_price")
      .cast("timestamp"))
    println("bigdata schema")
    bigdata.printSchema()
    bigdata.show
    PostgresUtils.saveDataframeIntoTable(spark, bigdata, s"ETL_BIGDATA")

    val metrics = bigdata.groupBy("month","year")
      .agg(max("price"),min("price"),
        max("price_official"),min("price_official"),
        avg("price"),avg("price_official"))
      .withColumnRenamed("MAX(PRICE)","MaxPricePerMonth")
      .withColumnRenamed("MIN(PRICE)","MinPricePerMonth")
      .withColumnRenamed("MAX(PRICE_OFFICIAL)","MaxPriceOfficialPerMonth")
      .withColumnRenamed("MIN(PRICE_OFFICIAL)","MinPriceOfficialPerMonth")
      .withColumnRenamed("AVG(PRICE)","AvgPricePerMonth")
      .withColumnRenamed("AVG(PRICE_OFFICIAL)","AvgPriceOfficialPerMonth")


    println("bigdata schema")
    metrics.printSchema()
    metrics.show
    PostgresUtils.saveDataframeIntoTable(spark, metrics, s"ETL_METRICS")



    println(s"ETL process comlete")

  }

}
