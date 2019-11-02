package task1

import org.apache.spark.sql.{DataFrame, SparkSession}
/*
 вывести топ 20 понравившихся, комментируемых, репостнутых постов по количеству (для обоих датасетов с постами)
 */
object Top20Posts {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("task1_Top20Posts").master("local[*]").getOrCreate()
    val df = sparkSession.read.json("data/posts_api.json/0_a99c9b7e_posts_api.json", "data/followers_posts_api_final.json").repartition(10)
    import sparkSession.implicits._

//    likes
    df.select("id", "owner_id","likes.count")
      .withColumnRenamed("id", "post_id")
      .withColumnRenamed("count", "likes_count")
      .orderBy($"likes_count".desc)
      .limit(20)
      .write.json("results/task1/top20_posts_by_likes")

//    reposts
    df.select("id", "owner_id","reposts.count")
      .withColumnRenamed("id", "post_id")
      .withColumnRenamed("count", "reposts_count")
      .orderBy($"reposts_count".desc)
      .limit(20)
      .write.json("results/task1/top20_posts_by_reposts")

//    comments
    df.select("id", "owner_id","comments.count")
      .withColumnRenamed("id", "post_id")
      .withColumnRenamed("count", "comments_count")
      .orderBy($"comments_count".desc)
      .limit(20)
      .write.json("results/task1/top20_posts_by_comments")
  }
}
