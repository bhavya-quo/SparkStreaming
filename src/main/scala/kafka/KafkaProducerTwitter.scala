package kafka

import java.util.Properties

import lib.Utilities._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.GeoLocation

/** Working example of listening for log data from Kafka's "topic1" topic on port 9091(broker 1's port). */
object KafkaProducerTwitter {

  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaExample", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "localhost:9091,anotherhost:9091",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "use_a_separate_group_id_for_each_stream",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
      .filter(tweet => tweet.getHashtagEntities.length > 0)

    // Blow out each word into a new DStream
//    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))
//
//    // Now eliminate anything that's not a hashtag
//    val hashtags = tweetwords.filter(word => word.startsWith("#"))
//
//    // Map each hashtag to a key/value pair of (hashtag, 1) so we can count them up by adding up the values
//    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))
//
//    // Now count them up over a 5 minute window sliding every one second
//    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(30), Seconds(1))
//    //  You will often see this written in the following shorthand:
//    //val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( _ + _, _ -_, Seconds(300), Seconds(1))
//
//    // Sort the results by the count values
//    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false)).map(x=>(x._1,x._2))

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
//          for (hashtag <- tweet._3) {
//            hashtags = hashtags + hashtag.getText
//          }
          if(tweet.getHashtagEntities.nonEmpty) {
            tweet.getHashtagEntities.foreach(hashtag=> {
              hashtags = hashtags + "\"" + hashtag.getText + "\""
              if(tweet.getHashtagEntities.indexOf(hashtag) != tweet.getHashtagEntities.length - 1) {
                hashtags += ","
              }
            })

          }
          hashtags += "]"
          val json = "{\"id\":"+tweet.getId+ ",\"hashtags\":" + hashtags + ",\"text\":\"" + tweet.getText + "\",\"timestamp\":" + tweet
            .getCreatedAt.getTime.toString +"}"
          val record = new ProducerRecord[String, String](topic, tweet.getCreatedAt.getTime.toString, json.toString)
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
