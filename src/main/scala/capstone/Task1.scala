package capstone

import capstone.TaskManager.{addSessions, pullNeedColumnsFromAppClickStreamsAttributesAndSessionId, pullNeedColumnsFromAppClickStreamsAttributesAndSessionIdAggregator, readCSVFile, saveDfAsCSV, saveDfAsParquet}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.DoubleType

object Task1 extends App {

  val outputParquet = args(0)
  val outputCSV = args(1)
  val outputForAggregatorsVersion = args(2)

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val appClickStreamDf: DataFrame = readCSVFile("src/resources/capstone-dataset/mobile_app_clickstream/*")
  val userPurchasesDf: DataFrame = readCSVFile("src/resources/capstone-dataset/user_purchases/*")

  val purchasesDf: DataFrame =
    userPurchasesDf.withColumn("tempColumn", userPurchasesDf("billingCost").cast(DoubleType))
      .drop("billingCost")
      .withColumnRenamed("tempColumn", "billingCost")

  val appClickStreamWithSessionsDf: DataFrame = addSessions(appClickStreamDf)
  appClickStreamWithSessionsDf.show()

  val projectionFromAppClickStreamDf: DataFrame = pullNeedColumnsFromAppClickStreamsAttributesAndSessionId(appClickStreamWithSessionsDf).persist()
  projectionFromAppClickStreamDf.show()
  val purchasesProjection: DataFrame = projectionFromAppClickStreamDf.join(purchasesDf, Seq("purchaseId")).persist()
  purchasesProjection.show()

  saveDfAsParquet(purchasesProjection, "spark-warehouse/" + outputParquet)
  saveDfAsCSV(purchasesProjection, "spark-warehouse/" + outputCSV)

  val projectionFromAggregatorDf = pullNeedColumnsFromAppClickStreamsAttributesAndSessionIdAggregator(appClickStreamWithSessionsDf)
  val purchasesProjectionFromAggregatorDf = projectionFromAggregatorDf.join(purchasesDf, Seq("purchaseId"))
  purchasesProjectionFromAggregatorDf.show()
  saveDfAsParquet(purchasesProjectionFromAggregatorDf, "spark-warehouse/" + outputForAggregatorsVersion)
}
