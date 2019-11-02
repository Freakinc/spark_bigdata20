package task1

import org.apache.spark.sql.SparkSession
/*
вывести топ 20 пользователей по сделанным лайкам и репостам (для репостов используйте "copy_history", из датасета постов подписчиков)
 */
object Top20UsersByLikesAndReposts {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("task1_Top20Posts").master("local[*]").getOrCreate()
    val df_likes = sparkSession.read.parquet("data/followers_posts_likes.parquet", "data/posts_likes.parquet").repartition(10)
    import sparkSession.implicits._

//    top 20 liking users
    df_likes.groupBy("likerId")
      .count()
      .withColumnRenamed("count", "likes_count")
      .withColumnRenamed("likerId", "user_id")
      .orderBy($"count".desc)
      .limit(20)
      .write.json("results/task1/top20_liking_users")

//    top 20 reposting users
    val df_reposts = sparkSession.read.json("data/followers_posts_api_final.json").repartition(10)
    df_reposts.filter($"copy_history".isNotNull)
      .groupBy("owner_id")
      .count()
      .orderBy($"count".desc)
      .limit(20)
      .write.json("results/task1/top20_reposting_users")
  }
}
