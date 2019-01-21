package ar.edu.itba.seminario

import org.apache.spark.sql.SparkSession

object GenerateData {
  def main(args: Array[String]): Unit = {

    val pricesTableName = "BCRA_PRICES"
    val pricesOficialTableName = "BCRA_PRICES_OFICIAL"
    val eventsTableName = "BCRA_EVENTS"
    val reservasTableName = "BCRA_RESERVAS"
    val spark = SparkSession.
      builder.
      appName("BCRA-Dolar:Generator").
      getOrCreate()

    ReadFromBCRA.readDataAndStoreInDatabase(spark, pricesTableName, "http://api.estadisticasbcra.com/usd")
    ReadFromBCRA.readDataAndStoreInDatabase(spark, pricesOficialTableName, "http://api.estadisticasbcra.com/usd_of")
    ReadFromBCRA.readDataAndStoreInDatabase(spark, eventsTableName, "http://api.estadisticasbcra.com/milestones")
    ReadFromBCRA.readDataAndStoreInDatabase(spark, reservasTableName, "http://api.estadisticasbcra.com/reservas")


  }

  import dispatch.Defaults._
  import dispatch._

  object ReadFromBCRA {
    private val API_KEY = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE1NzkzMDU3NzIsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJhbGVqYW5kcm8ubWlsZGluZXJAZ21haWwuY29tIn0.T7UfIVzF5TczkXvTzFKtCYUBUaMqBXRqXt7b-cQK0sHGgcC9YMW8hTR9HuMN8iZ2DyE6DBXVT2CkrP8_EqHgyw"

    def JSONToPostgres(spark: SparkSession, jsonStr: String, strTable: String): Unit = {
      val rdd = spark.sparkContext.parallelize(Seq(jsonStr))
      val df = spark.sqlContext.read.json(rdd)
        .withColumnRenamed("d", "fecha")
        .withColumnRenamed("e", "evento")
        .withColumnRenamed("t", "tipo")
        .withColumnRenamed("v", "precio")

      PostgresUtils.saveDataframeIntoTable(spark, df, strTable)

    }

    def readDataAndStoreInDatabase(spark: SparkSession, strTable: String, strUrl: String) = {
      println(s"""Reading data from BCRA api""".stripMargin)
      println(s"Requesting ${strTable}")
      
      val request = url(strUrl).GET
      val builtRequest = request.addHeader("Authorization", API_KEY)
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
      println(s"${builtRequest}")
      val content = Http(builtRequest)
      content onSuccess {
        case x if x.getStatusCode() == 200 => {
          JSONToPostgres(spark, x.getResponseBody, strTable)
        }
        case y => {
          println(s"Failed with status code ${y.getStatusCode()}")

        }
      }
      content onFailure {
        case x =>
          println(s"Failed but ${x.getMessage}")
      }
    }
  }
}
