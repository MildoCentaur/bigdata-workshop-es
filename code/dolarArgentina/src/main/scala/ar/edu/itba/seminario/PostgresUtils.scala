package ar.edu.itba.seminario

import java.sql.DriverManager

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

  def readTable(spark: SparkSession, strTable:String): DataFrame = {

    // Load the driver
    val connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD)
//    val query =  s"select BP.fecha fecha, BP.precio precio, BPO.precio precio_oficial, BE.evento evento, BE.tipo tipo from BCRA_PRICES BP inner join BCRA_PRICES_OFICIAL BPO on BP.fecha=BPO.fecha left  join BCRA_EVENTS BE on BE.fecha=BP.fecha"
    println(s"quering table: ${strTable}")
    val df =  spark.sqlContext.read.jdbc(JDBC_URL,strTable, getConnectionProperties())
    df

  }
}
