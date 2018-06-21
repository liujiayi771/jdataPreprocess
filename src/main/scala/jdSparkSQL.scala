import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

object jdSparkSQL {
  private var outputPath = ""
  private var dataPath = ""
  private var userOrderPath = ""
  private var userActionPath = ""
  private var userCommentScore = ""
  private var skuBasicInfoPath = ""
  private var userBasicInfoPath = ""

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: program data_path output_path")
      sys.exit(1)
    }
    dataPath = args(0)
    outputPath = args(1)
    userOrderPath = dataPath + "/jdata_user_order.csv"
    userActionPath = dataPath + "/jdata_user_action.csv"
    userCommentScore = dataPath + "/jdata_user_comment_score.csv"
    skuBasicInfoPath = dataPath + "/jdata_sku_basic_info.csv"
    userBasicInfoPath = dataPath + "/jdata_user_basic_info.csv"

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("jdSparkSQL")
      .getOrCreate()
    val f_startDate = "2016-07-01"
    val f_endDate = "2017-04-01"

    val l_startDate = "2017-04-01"
    val l_endDate = "2017-05-01"

    val splitDate = getTimeSplit(f_startDate, f_endDate)

    val areaFeatDF = getAreaFeat(spark, f_startDate, f_endDate)
    val userOrderSumFeatDF = getUserOrderSumFeat(spark, f_startDate, f_endDate)
    val userCommentSumFeatDF = getUserCommentSumFeat(spark, f_startDate, f_endDate)

    val userBoughtOrNotEarliestDateDF = getUserBoughtOrNotEarliestDate(spark, l_startDate, l_endDate)

    var df = userBoughtOrNotEarliestDateDF
      .join(userCommentSumFeatDF, Seq("user_id"), "left")
      .join(userOrderSumFeatDF, Seq("user_id"), "left")
      .join(areaFeatDF, Seq("user_id"), "left")

    for (split <- splitDate) {

      val actionFeatDF = getActionFeat(spark, split._2, split._3, split._1)
        .where(col("action_1_" + split._1).isNotNull || col("action_2_" + split._1).isNotNull)

      df = df
        .join(actionFeatDF, Seq("user_id"), "left")

    }
    df.persist(StorageLevel.MEMORY_ONLY)

    val colName = df.columns.toSet
    val labelColName = Set("user_id", "earliest_date", "bought")
    val featureColName = colName diff labelColName

    val featDF = df.select(("user_id" :: featureColName.toList.sorted).map(c => col(c)): _*)
    val labelDF = df.select(labelColName.toList.map(c => col(c)): _*)

    featDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/feat_" + f_startDate + "_" + f_endDate)

    labelDF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/label_" + l_startDate + "_" + l_endDate)

  }

  def getTimeSplit(startDate: String, endDate: String): Array[(Int, String, String)] = {
    val start = LocalDate.parse(startDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val end = LocalDate.parse(endDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val res = new ArrayBuffer[(Int, String, String)]()
    res += ((1, end.minusDays(1).toString, end.toString))
    res += ((3, end.minusDays(3).toString, end.toString))
    res += ((7, end.minusDays(7).toString, end.toString))
    res += ((15, end.minusDays(15).toString, end.toString))
    res += ((30, end.minusDays(30).toString, end.toString))
    res += ((60, end.minusDays(60).toString, end.toString))
    res += ((90, end.minusDays(90).toString, end.toString))
    res += ((0, startDate, endDate))

    res.toArray
  }

  def getAreaFeat(spark: SparkSession, startDate: String, endDate: String): DataFrame = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .withColumn("o_date", col("o_date").cast(DateType))
      .filter($"o_date".between(startDate, endDate))
      .select($"user_id", $"o_area")

    /* another method
    val countWindowSpec = Window.partitionBy($"user_id", $"o_area")
    val firstWindowSpec = Window.partitionBy($"user_id").orderBy($"count".desc)

    val resultDF = df
      .withColumn("count", count($"o_area").over(countWindowSpec))
      .withColumn("rn", row_number().over(firstWindowSpec))
      .where($"rn" === 1).drop("rn")
    */

    val dfWithCount = df
      .groupBy($"user_id", $"o_area")
      .agg(count($"o_area").as("count"))

    val windowSpec = Window.partitionBy($"user_id")

    val resultDF = dfWithCount.withColumn("maxCount", max($"count").over(windowSpec))
      .filter($"count" === $"maxCount")
      .drop("count")
      .groupBy($"user_id")
      .agg(first($"o_area").as("o_area"), first($"maxCount").as("max_count"))

    resultDF
  }

  def getActionFeat(spark: SparkSession, startDate: String, endDate: String, featureIndex: Int): DataFrame = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userActionPath)
      .withColumn("a_date", col("a_date").cast(DateType))
      .filter($"a_date".between(startDate, endDate))
      .select($"user_id", $"a_num", $"a_type")
      .withColumn("action_1", when(col("a_type") === 1, col("a_num")).otherwise(null))
      .withColumn("action_2", when(col("a_type") === 2, col("a_num")).otherwise(null))
      .groupBy($"user_id")
      .agg(sum($"action_1").as("action_1_" + featureIndex), sum($"action_2").as("action_2_" + featureIndex))

    df
  }

  def getMultipleActionFeat(spark: SparkSession, startDate: String, endDate: String): DataFrame = {
    import spark.implicits._
    val splitDate = getTimeSplit(startDate, endDate)

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userActionPath)
      .withColumn("a_date", col("a_date").cast(DateType))

    df.cache()
    var outDF = df
      .filter($"a_date".between(startDate, endDate))
      .select($"user_id", $"a_num", $"a_type")
      .withColumn("action_1", when(col("a_type") === 1, col("a_num")).otherwise(null))
      .withColumn("action_2", when(col("a_type") === 2, col("a_num")).otherwise(null))
      .groupBy($"user_id")
      .agg(sum($"action_1").as("action_1_0"), sum($"action_2").as("action_2_0"))

    for (split <- splitDate) {
      outDF = df
        .filter($"a_date".between(split._2, split._3))
        .select($"user_id", $"a_num", $"a_type")
        .withColumn("action_1", when(col("a_type") === 1, col("a_num")).otherwise(null))
        .withColumn("action_2", when(col("a_type") === 2, col("a_num")).otherwise(null))
        .groupBy($"user_id")
        .agg(sum($"action_1").as("action_1_" + split._1), sum($"action_2").as("action_2_" + split._1))
        .join(outDF, Seq("user_id"), "full")
    }
    outDF
  }

  def getUserOrderSumFeat(spark: SparkSession, startDate: String, endDate: String): DataFrame = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .withColumn("o_date", col("o_date").cast(DateType))
      .filter($"o_date".between(startDate, endDate))
      .select($"user_id", $"o_sku_num")
      .groupBy($"user_id")
      .agg(sum($"o_sku_num").as("o_sku_sum"))

    df
  }

  def getUserCommentSumFeat(spark: SparkSession, startDate: String, endDate: String): DataFrame = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userCommentScore)
      .withColumn("comment_create_tm", col("comment_create_tm").cast(DateType))
      .filter($"comment_create_tm".between(startDate, endDate))
      .select($"user_id", $"score_level")
      .filter($"score_level" > 0)
      .withColumn("score_level_1", when(col("score_level") === 1, 1).otherwise(null))
      .withColumn("score_level_2", when(col("score_level") === 2, 1).otherwise(null))
      .withColumn("score_level_3", when(col("score_level") === 3, 1).otherwise(null))
      .groupBy($"user_id")
      .agg(sum($"score_level_1").as("score_level_1"), sum($"score_level_2").as("score_level_2"), sum($"score_level_3").as("score_level_3"))

    df
  }

  def getUserBoughtOrNotEarliestDate(spark: SparkSession, startDate: String, endDate: String): DataFrame = {
    import spark.implicits._

    val userOrderDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .withColumn("o_date", col("o_date").cast(DateType))
      .filter($"o_date".between(startDate, endDate))

    val userBasicInfoDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userBasicInfoPath)

    val df = userBasicInfoDF.join(userOrderDF, userBasicInfoDF("user_id") === userOrderDF("user_id"), "full_outer")
      .select(userBasicInfoDF("user_id"), userOrderDF("o_date"))
      .groupBy($"user_id")
      .agg(min($"o_date").as("earliest_date"))
      .withColumn("bought", when(col("earliest_date").isNull, 0).otherwise(1))

    df
  }
}
