package kafka

import java.util.Properties

import lib.Utilities._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaTwitterProducer {

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
//      .filter(tweet => tweet.getHashtagEntities.length > 0 && tweet.getLang=="en")

    tweets.foreachRDD(tweetRDD => {
      if (tweetRDD.count() > 0) {
        val repartitionedRDD = tweetRDD.repartition(1).cache()
        repartitionedRDD.foreach(tweet => {
          val props:Properties = new Properties()
          props.put("bootstrap.servers","localhost:9092")
          props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer")
          props.put("acks","all")
          @transient val producer = new KafkaProducer[String, String](props)
          @transient val topic = "twitter"

          var hashtags: String = "["
          if(tweet.getHashtagEntities.nonEmpty) {
            tweet.getHashtagEntities.foreach(hashtag=> {
              hashtags = hashtags + "\"" + hashtag.getText + "\""
              if(tweet.getHashtagEntities.indexOf(hashtag) != tweet.getHashtagEntities.length - 1) {
                hashtags += ","
              }
            })

          }
          hashtags += "]"
          val json = "{\"id\":"+tweet.getId+ ",\"hashtags\":" + hashtags + ",\"text\":\"" + tweet.getText.replace("\"","\\\"") + "\",\"timestamp\":" +
            tweet
            .getCreatedAt.getTime.toString +"}"
          val record = new ProducerRecord[String, String](topic, tweet.getCreatedAt.getTime.toString, json.split('\n').map(_.trim.filter(_ >= ' ')).mkString)
          val metadata = producer.send(record)
          printf(s"sent record(key=%s value=%s) " +
            "meta(partition=%d, offset=%d)\n",
            record.key(), record.value(),
            metadata.get().partition(),
            metadata.get().offset())
        })
      }
    })

    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("/tmp/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
