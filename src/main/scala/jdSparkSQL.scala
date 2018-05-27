import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object jdSparkSQL {
  private val projectPath = "/home/joey/Documents/projects/IdeaProjects/jdSparkSQL"
  private val userOrderPath = projectPath + "/data/jdata_user_order.csv"
  private val userActionPath = projectPath + "/data/jdata_user_action.csv"
  private val userCommentScore = projectPath + "/data/jdata_user_comment_score.csv"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("spark://joey:7077")
      .appName("jdSparkSQL")
      .getOrCreate()
    val startDate = "2016-05-01"
    val endDate = "2017-01-31"
    getUserCommentSumFeat(spark, startDate, endDate)

  }

  def getAreaFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .filter($"o_date".cast(DateType).between(startDate, endDate))
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

    resultDF.show(100)
  }

  def getActionFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userActionPath)
      .filter($"a_date".cast(DateType).between(startDate, endDate))
      .select($"user_id", $"a_num", $"a_type")
      .withColumn("action_1", when(col("a_type") === 1, col("a_num")).otherwise(0))
      .withColumn("action_2", when(col("a_type") === 2, col("a_num")).otherwise(0))
      .groupBy($"user_id")
      .agg(sum($"action_1").as("action_1"), sum($"action_2").as("action_2"))

    df.show(100)
  }

  def getUserOrderSumFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userOrderPath)
      .filter($"o_date".cast(DateType).between(startDate, endDate))
      .select($"user_id", $"o_sku_num")
      .groupBy($"user_id")
      .agg(sum($"o_sku_num").as("o_sku_sum"))

    df.show(100)
  }

  def getUserCommentSumFeat(spark: SparkSession, startDate: String, endDate: String): Unit = {
    import spark.implicits._

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file://" + userCommentScore)
      .filter($"comment_create_tm".cast(DateType).between(startDate, endDate))
      .select($"user_id", $"score_level")
      .filter($"score_level" > 0)
      .withColumn("score_level_1", when(col("score_level") === 1, 1).otherwise(0))
      .withColumn("score_level_2", when(col("score_level") === 2, 1).otherwise(0))
      .withColumn("score_level_3", when(col("score_level") === 3, 1).otherwise(0))
      .groupBy($"user_id")
      .agg(sum($"score_level_1").as("score_level_1"), sum($"score_level_2").as("score_level_3"), sum($"score_level_1").as("score_level_2"))

    df.show(100)
  }
}
