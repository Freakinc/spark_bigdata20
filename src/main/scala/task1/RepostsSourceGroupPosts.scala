package task1

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{collect_list, explode, size}

/*
получить репосты исходных постов группы itmo (posts.json) из постов пользователей
(результат должен быть похож на (group_post_id, Array (user_post_ids)) )
 */
object RepostsSourceGroupPosts {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("task1_RepostsSourceGroupPosts").master("local[*]").getOrCreate()
    import sparkSession.implicits._

//    собираем посты подписчиков, которые являются репостами
    val followers_posts = sparkSession.read.json("data/followers_posts_api_final.json")
      .select("from_id", "copy_history")
      .filter(row => row.get(1) != null)
      .withColumn("copy_history", explode($"copy_history"))

//    берём посты, для которых copy_history.owner_id = -94 - т.е. репосты из группы ITMO
    val posts = followers_posts.select("from_id", "copy_history.id", "copy_history.owner_id")
      .filter("copy_history.owner_id == -94")
      .toDF("from_id", "id", "owner")
      .sort($"id")

//    приводим данные к виду "group_post_id, Array (user_post_ids)"
    posts.groupBy("id")
      .agg(collect_list("from_id"))
      .withColumnRenamed("id", "group_post_id")
      .withColumnRenamed("collect_list(from_id)", "user_post_ids")
      .orderBy(size($"user_post_ids").desc)
      .limit(20)
      .write.json("results/task1/reposts_source")
  }
}
