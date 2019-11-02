package task1

import org.apache.spark.sql.SparkSession

/*
получить репосты исходных постов группы itmo (posts.json) из постов пользователей
(результат должен быть похож на (group_post_id, Array (user_post_ids)) )
 */
object RepostsSourceGroupPosts {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("task1_Top20Posts").master("local[*]").getOrCreate()
    val df_group_posts = sparkSession.read.json("data/posts_api.json").repartition(10)
    val df_follower_posts = sparkSession.read.json("data/followers_posts_api_final.json").repartition(10)
    import sparkSession.implicits._

    val listIds = df_group_posts.filter($"id".isNotNull && $"reposts.count".gt(0))
      .select("id")
      .map(id => id.getLong(0))
      .collectAsList()

    df_follower_posts.filter($"copy_history".isNotNull)
      .select("copy_history.from_id")
      .map(id => id.getLong(0))
      .filter(from_id => listIds.contains(from_id))
      .show()
  }
}
