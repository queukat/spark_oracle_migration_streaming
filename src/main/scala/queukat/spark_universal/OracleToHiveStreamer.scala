package queukat.spark_universal

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

class OracleToHiveStreamer(spark: SparkSession, oracleUrl: String, oracleUser: String, oraclePassword: String) {

  private var accumulatedDataFrame: DataFrame = _
  private var accumulatedSize: Long = 0L
  private val hdfsBlockSize: Long = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", 128 * 1024 * 1024) // Получение размера блока HDFS

  def readFromOracle(query: String): DataFrame = {

    spark.read
      .format("jdbc")
      .option("url", oracleUrl)
      .option("user", oracleUser)
      .option("password", oraclePassword)
      .option("dbtable", s"($query) as oracle_table")
      .load()
  }

  def accumulateAndProcessData(dataFrame: DataFrame, hivetable: String): Unit = {
    if (accumulatedDataFrame == null) {
      accumulatedDataFrame = dataFrame.cache()
    } else {
      accumulatedDataFrame = accumulatedDataFrame.union(dataFrame).cache()
    }

    // Обновление размера накопленных данных
    accumulatedSize += dataFrame.count() * approximateRowSize(dataFrame)

    if (accumulatedSize >= hdfsBlockSize) {
      writeToHive(accumulatedDataFrame, hivetable)
      resetAccumulator()
    }
  }

  private def approximateRowSize(dataFrame: DataFrame): Long = {
    // Оценка размера строки в DataFrame
    100L // Примерный размер строки в байтах
  }

  private def writeToHive(dataFrame: DataFrame, hivetable: String): Unit = {
    // Запись накопленных данных в Hive
    dataFrame.write.mode("append").saveAsTable(hivetable)
  }

  private def resetAccumulator(): Unit = {
    // Сброс накопленных данных
    if (accumulatedDataFrame != null) {
      accumulatedDataFrame.unpersist()
    }
    accumulatedDataFrame = null
    accumulatedSize = 0L
  }
}
