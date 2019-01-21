package ar.edu.itba.seminario

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object RunAll {
  def main(args: Array[String]): Unit = {
    //    if (args.length < 3) {
    //      System.err.println(
    //        s"""
    //           |Usage: RunAll <dataset folder> <lookup file> <output folder>
    //           |  <dataset folder> folder where stocks data is located
    //           |  <lookup file> file containing lookup information
    //           |  <output folder> folder to write parquet data
    //           |
    //           |RunAll /dataset/stocks-small /dataset/yahoo-symbols-201709.csv /dataset/output.parquet
    //        """.stripMargin)
    //      System.exit(1)
    //    }

    //    val Array(stocksFolder, lookupSymbol, outputFolder) = args

    val pricesTableName = "BCRA_PRICES"
    val pricesOficialTableName = "BCRA_PRICES_OFICIAL"
    val eventsTableName = "BCRA_EVENTS"
    val spark = SparkSession.
      builder.
      appName("BCRA-Dolar:ETL").
      getOrCreate()

    //ReadFromBCRA.readDataAndStoreInDatabase(spark, pricesTableName, "http://api.estadisticasbcra.com/usd")
    //ReadFromBCRA.readDataAndStoreInDatabase(spark, pricesOficialTableName, "http://api.estadisticasbcra.com/usd_of")
    //ReadFromBCRA.readDataAndStoreInDatabase(spark, eventsTableName, "http://api.estadisticasbcra.com/milestones")
    //spark.stop()
  }
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


    df.printSchema()
    df.show()
    if(strTable.equals("BCRA_EVENTS")){
      df.write.format("com.databricks.spark.csv").save("/dataset/economicEvents.csv")
    }
    PostgresUtils.saveDataframeIntoTable(spark, df, strTable)

  }

  def readDataAndStoreInDatabase(spark: SparkSession, strTable: String, strUrl: String) = {
    println(s"""Reading data from BCRA api""".stripMargin)
    println(s"Requesting prices")
    //Extracted from http://krishnabhargav.github.io/scala,/how/to/2014/06/15/Scala-Json-REST-Example.html
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



  };
  case class EconomicEvent(fecha: String,
                           evento: String,
                           tipo: String)

  object EconomicEvent {
    def fromCSV(symbol: String, line: String): Option[EconomicEvent] = {
      val v = line.split(",")

      try {
        Some(
          EconomicEvent(
            fecha = v(0),
            evento = v(1),
            tipo = v(2)
          )
        )

      } catch {
        case ex: Exception => {
          println(s"Failed to process $symbol, with input $line, with ${ex.toString}")
          None
        }
      }

    }
  }
  object ReadEconomicEventCSV {

    def extractSymbolFromFilename(filename: String) = {
      val arr = filename.split("/")
      arr(arr.size - 1).split("\\.")(0).toUpperCase
    }

    def processDS(spark: SparkSession, originFolder: String) = {
      import spark.implicits._


      spark.read.
        option("header", true).
        option("inferSchema", true).
        csv(originFolder).
        as[EconomicEvent]
    }


    def processRDD(spark: SparkSession, originFolder: String) = {

      // Using SparkContext to use RDD
      val sc = spark.sparkContext
      val files = sc.wholeTextFiles(originFolder, minPartitions = 40)

      val stocks = files.map { case (filename, content) =>
        val symbol = extractSymbolFromFilename(filename)

        content.split("\n").flatMap { line =>
          EconomicEvent.fromCSV(symbol, line)
        }
      }.
        flatMap(e => e).
        cache

      import spark.implicits._

      stocks.toDS.as[EconomicEvent]
    }
  }

  import java.sql.DriverManager

  object PostgresUtils {
    val JDBC_URL = "jdbc:postgresql://postgres:5432/workshop"
    val DRIVER = "org.postgresql.Driver"
    val USERNAME = "workshop"
    val PASSWORD = "w0rkzh0p"

    def getConnectionProperties(): java.util.Properties = {
      val connectionProperties = new java.util.Properties
      connectionProperties.put("user", "workshop")
      connectionProperties.put("password", "w0rkzh0p")

      connectionProperties
    }

    def saveDataframeIntoTable(spark: SparkSession, df: DataFrame, strTable: String): Unit = {
      // Write to Postgres
      val connectionProperties = getConnectionProperties()
      val jdbcUrl = s"jdbc:postgresql://postgres:5432/workshop"

      df.
        write.
        mode(SaveMode.Overwrite).
        jdbc(jdbcUrl, strTable, connectionProperties)

    }

    def mergeData(spark: SparkSession): Unit = {

      // Load the driver
      val connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)
      val query =  s"select BP.fecha fecha, BP.precio precio, BPO.precio precio_oficial, BE.evento evento, BE.tipo tipo from BCRA_PRICES BP inner join BCRA_PRICES_OFICIAL BPO on BP.fecha=BPO.fecha left  join BCRA_EVENTS BE on BE.fecha=BP.fecha"
      println(s"query: ${query}")
      val myRDD = new JdbcRDD(spark.sparkContext, () => connection, query, 1, 100, 10,
        r => r.getString(0) + ", " + r.getString(1) + "," + r.getString(2) + ", " + r.getString(3) + ", " + r.getString(4))
      myRDD.getPartitions
      val df = spark.sqlContext.read.json(myRDD)
      df.show()
      df.printSchema()
      //      val df =  spark.sqlContext.read.jdbc(JDBC_URL,query,Array.empty[String], getConnectionProperties())
      saveDataframeIntoTable(spark,df,"big_table")

    }
  }

}
