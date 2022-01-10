Streams job posting tweets from twitter and produces them in kafka stream/topic

Note: Need to have twitter developer account. Keys/Tokens present in the code files are expired ones.

Important commands


###KAFKA#########

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-consumer.sh --topic twitter-job-tweets-2 --from-beginning --bootstrap-server localhost:9092 --partition 0





###SPARK#########

./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.24 --master local[2] /home/vishal/spark_practice/kafka_tweet_stream.py


###DOCKER DB#########

to initialize: sudo docker run --name postgresdb -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres
sudo docker start postgresdb


  server: localhost,
  port: 5432,
  db: twitterjobdb,
  uid: postgres,
  pwd: postgres
