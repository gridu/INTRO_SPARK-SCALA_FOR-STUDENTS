package capstone

import capstone.TaskManager.{popularChannelsSQL, popularChannelsSparkAPI, readParquetFile, saveDfAsParquet, topTenCampaignsSQL, topTenCampaignsSparkAPI}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2 extends App {
  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val purchasesProjection: DataFrame = readParquetFile("spark-warehouse/purchasesProjection/*")
  purchasesProjection.show()

  val topTenCampaignsSQLDf = topTenCampaignsSQL(purchasesProjection)
  topTenCampaignsSQLDf.show()
  saveDfAsParquet(topTenCampaignsSQLDf, "spark-warehouse/queryOutput/topTenCampaignsSQL")
  readParquetFile("spark-warehouse/queryOutput/topTenCampaignsSQL/*").show()

  val topTenCampaignsSparkAPIDf = topTenCampaignsSparkAPI(purchasesProjection)
  topTenCampaignsSparkAPIDf.show()
  saveDfAsParquet(topTenCampaignsSparkAPIDf, "spark-warehouse/queryOutput/topTenCampaignsSparkAPI")
  readParquetFile("spark-warehouse/queryOutput/topTenCampaignsSparkAPI/*").show()

  val popularChannelsSQLDf = popularChannelsSQL(purchasesProjection)
  popularChannelsSQLDf.show()
  saveDfAsParquet(popularChannelsSQLDf, "spark-warehouse/queryOutput/popularChannelsSQL")
  readParquetFile("spark-warehouse/queryOutput/popularChannelsSQL/*").show()

  val popularChannelsSparkAPIDf = popularChannelsSparkAPI(purchasesProjection)
  popularChannelsSparkAPIDf.show()
  saveDfAsParquet(popularChannelsSparkAPIDf, "spark-warehouse/queryOutput/popularChannelsSparkAPI")
  readParquetFile("spark-warehouse/queryOutput/popularChannelsSparkAPI/*").show()
}
