import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object jdSparkSQL {
  private val outputPath = "/Users/joey/Documents/projects/github/jdataPreprocess/data"
  private val projectPath = "/Users/joey/Documents/projects/github/jdataPreprocess"
  private val userOrderPath = projectPath + "/data/jdata_user_order.csv"
  private val userActionPath = projectPath + "/data/jdata_user_action.csv"
  private val userCommentScore = projectPath + "/data/jdata_user_comment_score.csv"
  private val skuBasicInfoPath = projectPath + "/data/jdata_sku_basic_info.csv"
  private val userBasicInfoPath = projectPath + "/data/jdata_user_basic_info.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("jdSparkSQL")
      .getOrCreate()
    val startDate = "2016-05-01"
    val endDate = "2017-01-31"
    getUserBoughtOrNotEarliestDate(spark, startDate, endDate)

  }

  def getAreaFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .withColumn("o_date", col("o_date").cast(DateType))
      .filter($"o_date".between(startDate, endDate))
      .select($"user_id", $"o_area")

//    val countWindowSpec = Window.partitionBy($"user_id", $"o_area")
//    val firstWindowSpec = Window.partitionBy($"user_id").orderBy($"count".desc)
//
//    val resultDF = df
//      .withColumn("count", count($"o_area").over(countWindowSpec))
//      .withColumn("rn", row_number().over(firstWindowSpec))
//      .where($"rn" === 1).drop("rn")


    val dfWithCount = df
      .groupBy($"user_id", $"o_area")
      .agg(count($"o_area").as("count"))

    val windowSpec = Window.partitionBy($"user_id")

    val resultDF = dfWithCount.withColumn("maxCount", max($"count").over(windowSpec))
      .filter($"count" === $"maxCount")
      .drop("count")
      .groupBy($"user_id")
      .agg(first($"o_area").as("o_area"), first($"maxCount").as("maxCount"))

    resultDF.coalesce(1)
      .write
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/area_feat.csv")
  }

  def getActionFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userActionPath)
      .withColumn("a_date", col("a_date").cast(DateType))
      .filter($"a_date".between(startDate, endDate))
      .select($"user_id", $"a_num", $"a_type")
      .withColumn("action_1", when(col("a_type") === 1, col("a_num")).otherwise(0))
      .withColumn("action_2", when(col("a_type") === 2, col("a_num")).otherwise(0))
      .groupBy($"user_id")
      .agg(sum($"action_1").as("action_1"), sum($"action_2").as("action_2"))

    df.coalesce(1)
      .write
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/action_feat.csv")
  }

  def getUserOrderSumFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
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

    df.coalesce(1)
      .write
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/user_order_sum_feat.csv")
  }

  def getUserCommentSumFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userCommentScore)
      .withColumn("comment_create_tm", col("comment_create_tm").cast(DateType))
      .filter($"comment_create_tm".between(startDate, endDate))
      .select($"user_id", $"score_level")
      .filter($"score_level" > 0)
      .withColumn("score_level_1", when(col("score_level") === 1, 1).otherwise(0))
      .withColumn("score_level_2", when(col("score_level") === 2, 1).otherwise(0))
      .withColumn("score_level_3", when(col("score_level") === 3, 1).otherwise(0))
      .groupBy($"user_id")
      .agg(sum($"score_level_1").as("score_level_1"), sum($"score_level_2").as("score_level_3"), sum($"score_level_1").as("score_level_2"))

    df.coalesce(1)
      .write
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/user_comment_sum_feat.csv")
  }

  def getUserBoughtOrNotEarliestDate(spark: SparkSession, startDate: String, endDate: String): Unit = {
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

    df.coalesce(1)
      .write
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + outputPath + "/user_bought_or_not_earliest_date.csv")
  }
}
