package capstone

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.functions.{col, countDistinct, desc, element_at, lit, row_number, when}
import org.apache.spark.sql.types.{MapType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession, TypedColumn, functions}

import java.time.LocalDate

object TaskManager {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Capstone")
      .master("local")
      .getOrCreate()

  def readCSVFile(path: String)(implicit spark: SparkSession): DataFrame =
    spark.read
      .options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true"))
      .csv(path)

  def saveDfAsCSV(df: DataFrame, path: String): Unit = {
    df.repartition(1)
      .write
      .mode("overwrite")
      .format("csv")
      .save(path)
  }

  def saveDfAsParquet(df: DataFrame, path: String): Unit = {
    //    df.write.parquet(path)
    df.repartition(1)
      .write
      .mode("overwrite")
      .format("parquet")
      .save(path)
  }

  def readParquetFile(path: String): DataFrame = {
    spark.read.parquet(path)
  }

  def addSessions(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val eventTypeFlg: Column = {
      when($"eventType" === "app_open", 1)
        .otherwise(0)
        .as("eventTypeFlg")
    }
    val windowSession = Window.partitionBy("userId").orderBy("eventTime")

    df.withColumn("eventTypeFlg", eventTypeFlg)
      .withColumn("userSessionId", functions.sum($"eventTypeFlg").over(windowSession))
      .withColumn("sessionId", functions.concat($"userId", lit("-"), $"userSessionId"))
      .withColumn("attr", functions.from_json($"attributes", MapType(StringType, StringType)))
  }

  def pullNeedColumnsFromAppClickStreamsAttributesAndSessionId(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val windowFunctionForPartitioningBySessionId = Window.partitionBy("sessionId")
    df
      .withColumn("campaignId", element_at($"attr", "campaign_id"))
      .withColumn("channelId", element_at($"attr", "channel_id"))
      .withColumn("pre_purchaseId", element_at($"attr", "purchase_id"))
      .withColumn("purchaseId", functions.max($"pre_purchaseId").over(windowFunctionForPartitioningBySessionId))
      .filter($"eventTypeFlg" === 1)
      .select($"sessionId", $"campaignId", $"channelId", $"purchaseId")
  }

  //  def pullNeedColumnsFromAppClickStreamsAttributesAndSessionId(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
  //    import spark.implicits._
  //
  //    val windowFunctionForPartitioningBySessionId = Window.partitionBy("sessionId")
  //    df
  //      .withColumn("campaignId", element_at($"attr", "campaign_id"))
  //      .withColumn("channelId", element_at($"attr", "channel_id"))
  //      .withColumn("pre_purchaseId", element_at($"attr", "purchase_id"))
  //      .withColumn("purchaseId", functions.max($"pre_purchaseId").over(windowFunctionForPartitioningBySessionId))
  //      //      .filter($"eventTypeFlg" === 1)
  //      .filter($"sessionId".startsWith("000d7e7f-682c"))
  //      //Wrap '(max(`pre_campaignId`) AS `campaignId`)' in windowing function(s)
  //      //.orderBy("sessionId")
  //      // select some necessary columns for the projection
  //      .select($"sessionId", $"campaignId", $"channelId", $"pre_purchaseId", $"purchaseId", $"eventTypeFlg")
  //  }

  type StringMap = Map[String, String]

  case class SessionAttrs(sessionID: String, attr: StringMap)

  val MapAggregator: TypedColumn[SessionAttrs, StringMap] = new Aggregator[SessionAttrs, StringMap, StringMap] {
    def zero: StringMap = Map.empty[String, String]

    def reduce(accum: StringMap, a: SessionAttrs): StringMap = accum ++ a.attr

    def merge(map1: StringMap, map2: StringMap): StringMap = map1 ++ map2

    def finish(result: StringMap): StringMap = result

    def bufferEncoder: Encoder[StringMap] = ExpressionEncoder()

    def outputEncoder: Encoder[StringMap] = ExpressionEncoder()
  }.toColumn

  def pullNeedColumnsFromAppClickStreamsAttributesAndSessionIdAggregator(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val ds = df.select($"sessionId", $"attr")
      .na.drop()
      .as[SessionAttrs]

    ds.groupByKey(_.sessionID)
      .agg(MapAggregator.name("attrs"))
      .withColumn("campaignId", element_at($"attrs", "campaign_id"))
      .withColumn("channelId", element_at($"attrs", "channel_id"))
      .withColumn("purchaseId", element_at($"attrs", "purchase_id"))
      .withColumnRenamed("value", "sessionId")
      .select($"sessionId", $"campaignId", $"channelId", $"purchaseId")
      .orderBy("sessionId")
  }

  def topTenCampaignsSQL(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT campaignId, " +
      "SUM(billingCost) AS sumBillingCost " +
      "FROM purchasesProjection " +
      "WHERE isConfirmed = true " +
      "GROUP BY campaignId " +
      "ORDER BY sumBillingCost DESC " +
      "LIMIT 10"
    )
  }

  def topTenCampaignsByPeriodOct11SQL(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT campaignId, purchaseTime, SUM(billingCost) AS sumByPeriodBillingCost " +
      "FROM purchasesProjection " +
      "WHERE isConfirmed = true " +
      "AND purchaseTime > '2020-11-10' AND purchaseTime < '2020-11-12' " +
      "GROUP BY purchaseTime, campaignId ORDER BY purchaseTime " +
      "LIMIT 10")
  }

  def topTenCampaignsByPeriodOct11SqlVersion2(df: DataFrame, localDate: LocalDate)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT campaignId, " +
      "purchaseTime, " +
      "SUM(billingCost) AS sumByPeriodBillingCost " +
      "FROM purchasesProjection " +
      "WHERE isConfirmed = 'true' " +
      "AND DAY('" + localDate + "') = EXTRACT(DAY FROM purchaseTime) "+
      "AND MONTH('" + localDate + "') = EXTRACT(MONTH FROM purchaseTime) "+
      "AND YEAR('" + localDate + "') = EXTRACT(YEAR FROM purchaseTime) "+
      "GROUP BY campaignId, purchaseTime " +
      "ORDER BY SUM(billingCost) DESC " +
      "LIMIT 10")
  }

  def topTenCampaignsByPeriodSepSQL(df: DataFrame, localDate: String, localDate2: String)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT campaignId, " +
      "purchaseTime, " +
      "SUM(billingCost) AS sumByPeriodBillingCost " +
      "FROM purchasesProjection " +
      "WHERE isConfirmed = 'true' " +
      "AND DATE(purchaseTime) " +
      "BETWEEN DATE('" + localDate + "') AND DATE('" + localDate2 + "') " +
      "GROUP BY campaignId, purchaseTime " +
      "ORDER BY SUM(billingCost) DESC " +
      "LIMIT 10")
  }

  def topTenCampaignsSparkAPI(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.filter($"isConfirmed" === true)
      .groupBy("campaignId")
      .sum("billingCost")
      .withColumnRenamed("sum(billingCost)", "sumBillingCost")
      .orderBy($"sumBillingCost".desc)
      .limit(10)
  }

  def popularChannelsSQL(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT campaignId, channelId " +
      "FROM " +
      "(SELECT campaignId, " +
      "channelId, " +
      "sessions, " +
      "ROW_NUMBER() OVER (PARTITION BY campaignId ORDER BY sessions DESC) " +
      "AS rowNumber " +
      "FROM " +
      "(SELECT campaignId, " +
      "channelId, " +
      "COUNT(distinct(sessionId)) " +
      "AS sessions " +
      "FROM purchasesProjection " +
      "GROUP BY campaignId, channelId " +
      "ORDER BY sessions DESC)) " +
      "WHERE rowNumber = 1 ORDER BY campaignId")
  }

  def popularChannelsSparkAPI(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val window = Window.partitionBy($"campaignId").orderBy($"sessions".desc)

    df.groupBy($"campaignId", $"channelId")
      .agg(countDistinct("sessionId").as("sessions"))
      .withColumn("rowNumber", row_number().over(window))
      .filter($"rowNumber" === 1)
      .select("campaignId", "channelId")
      .orderBy($"campaignId")
  }

  def popularChannelsByPeriodSQL(df: DataFrame, localDate: LocalDate)(implicit spark: SparkSession): DataFrame = {
    df.createOrReplaceTempView("purchasesProjection")

    spark.sql("" +
      "SELECT * " +
      "FROM " +
      "(SELECT ROW_NUMBER() OVER(PARTITION BY campaignId ORDER BY countBillingCost DESC) AS rowNumber, " +
      "campaignId, " +
      "channelId, " +
      "countBillingCost " +
      "FROM (SELECT campaignId, " +
      "channelId, " +
      "COUNT(channelId) AS countBillingCost " +
      "FROM purchasesProjection " +
      "WHERE DAY('" + localDate + "') = EXTRACT(DAY FROM purchaseTime) "+
      "AND MONTH('" + localDate + "') = EXTRACT(MONTH FROM purchaseTime) "+
      "AND YEAR('" + localDate + "') = EXTRACT(YEAR FROM purchaseTime) "+
      "GROUP BY campaignId, channelId))" +
      "WHERE rowNumber = 1")
  }
}
