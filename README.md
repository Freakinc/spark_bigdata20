### Big Data ITMO course

# Install SBT
https://www.scala-sbt.org/download.html

## Task1 
The dataset contains data from the itmo community vk.com (https://vk.com/club94) and is located at https://cloud.mail.ru/public/29hd/4wctpKRwH
There are group posts, likes for posts, followers, followers posts from 2019 and its likes.
You can use Jupiter + PySpark or Idea + Scala + Spark
Each task should be in a separate file. For each task, there must be an output dataset in Json format.
Tasks:
1. Display the top 20 liked, commented, reposted posts by number (for all datasets with posts)
2. Display the top 20 users by likes and reposts made (for reposts use "copy_history", from the subset posts
3. Get reposts of the original posts of the itmo group (posts.json) from user posts (the result should be similar to (group_post_id, Array (user_post_ids)))
4. Find emoticons in posts and post comments (negative, positive, neutral) (you can use external libraries or predefined emoticon lists) (use the spark udf and broadcast function for emoticons)
5. Find friends. The idea is, if users like each other posts, then they are friends. (*bonus)
6. Find fans/lovers. The idea is, if the user likes another posts, and its is not mutual, then the first fan/lover. (*bonus)
