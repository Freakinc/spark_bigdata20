package task1

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
/*
айти смайлики в постах и комментариях к постам (негативные, позитивные, нейтральные)
(можете использовать внешние библиотеки или предопределенные списки смайликов)
(используйте функцию spark udf и broadcast для смайликов
 */
object SmilesInPostsDetection {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("task1_smilesInPostsDetection").master("local[*]").getOrCreate()

    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    val smiles = Map(
                     ("\uD83D\uDE21" -> "neg"), //      😡
                     ("\uD83E\uDD2C" -> "neg"), //      🤬
                     ("\uD83D\uDE0A" -> "pos"), //      😊
                     ("\uD83D\uDE02" -> "pos"), //      😂
                     ("\uD83D\uDE10" -> "neu"), //      😐
                     ("\uD83D\uDE05" -> "neu")) //      😅

    val broadCastDictionary = sc.broadcast(smiles)

    val pos_group_count = sc.accumulator(0)
    val neg_group_count = sc.accumulator(0)
    val neu_group_count = sc.accumulator(0)

    val group_posts = sparkSession.read
      .json("data/posts_api.json")
      .select("key", "text")
      .filter(row => row.get(1) != "")

    group_posts.foreach(row => {
      if (!row.isNullAt(1)) {
        val line = row.get(1).toString
        val words = line.split(" ").toList
        val temp = words.map(word => getElementsCount(word, broadCastDictionary))
        temp.foreach(row => {
          row._1 match {
            case "pos" => pos_group_count += row._2
            case "neg" => neg_group_count += row._2
            case "neu" => neu_group_count += row._2
            case _ => ""
          }
        })
      }
    })

    val pos_followers_count = sc.accumulator(0)
    val neg_followers_count = sc.accumulator(0)
    val neu_followers_count = sc.accumulator(0)

    val followers_posts = sparkSession.read
      .json("data/followers_posts_api_final.json")
      .select("key", "text")
      .filter(row => row.get(1) != "")

    followers_posts.foreach(row => {
      if (!row.isNullAt(1)) {
        val str = row.get(1).toString
        val words = str.split(" ").toList
        val temp = words.map(word => getElementsCount(word, broadCastDictionary))
        temp.foreach(row => {
          row._1 match {
            case "pos" => pos_followers_count += row._2
            case "neg" => neg_followers_count += row._2
            case "neu" => neu_followers_count += row._2
            case _ => ""
          }
        })
      }
    })

    val result = Seq(
      ("Group posts", pos_group_count.value, neg_group_count.value, neu_group_count.value),
      ("Followers posts", pos_followers_count.value, neg_followers_count.value, neu_followers_count.value)
      ).toDF("from", "pos", "neg", "neu")

    result.write.json("results/task1/smiles_in_posts_detection")

  }

  private def getElementsCount(word: String, dict: Broadcast[Map[String, String]]): (String, Int) = {
    dict.value
      .filter { case (wording, wordType) => wording.equals((word)) }
      .map(x => (x._2, 1))
      .headOption
      .getOrElse(("undefined" -> 1))
  }
}
