package capstone

import capstone.TaskManager.{popularChannelsByPeriodSQL, readCSVFile, readParquetFile, topTenCampaignsByPeriodOct11SQL, topTenCampaignsByPeriodOct11SqlVersion2, topTenCampaignsByPeriodSepSQL}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

object Task3 extends App {
  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val purchasesProjection: DataFrame = readParquetFile("spark-warehouse/purchasesProjection/*")
  readParquetFile("spark-warehouse/purchasesProjection/*").explain(extended = true)
  spark.time(readParquetFile("spark-warehouse/purchasesProjection/*"))

  val purchasesProjectionCSV: DataFrame = readCSVFile("spark-warehouse/purchasesProjectionCSV/*")
  readCSVFile("spark-warehouse/purchasesProjectionCSV/*").explain(extended = true)
  spark.time(readCSVFile("spark-warehouse/purchasesProjectionCSV/*"))

  val dateEleventhNovember = LocalDate.parse("2020-11-11")
  val dateEleventhSep1 = LocalDate.parse("2020-09-01")
  val dateEleventhSep30 = LocalDate.parse("2020-09-30")

  val topTenCampaignsByPeriodOct11SQLDf = topTenCampaignsByPeriodOct11SQL(purchasesProjection)
  topTenCampaignsByPeriodOct11SQLDf.show()

  val topTenCampaignsByPeriodOct11SqlVersion2Df = topTenCampaignsByPeriodOct11SqlVersion2(purchasesProjection, dateEleventhNovember)
  topTenCampaignsByPeriodOct11SqlVersion2Df.show()

  val topTenCampaignsByPeriodSepSQLDf = topTenCampaignsByPeriodSepSQL(purchasesProjection)
  topTenCampaignsByPeriodSepSQLDf.show()

  val popularChannelsByPeriodSQLDf = popularChannelsByPeriodSQL(purchasesProjection, dateEleventhNovember)
  popularChannelsByPeriodSQLDf.show()
}
