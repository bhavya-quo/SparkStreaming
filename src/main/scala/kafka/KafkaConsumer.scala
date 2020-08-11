package kafka

import org.apache.spark.streaming.{Seconds, StreamingContext}

import lib.Utilities._

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/** Working example of listening for log data from Kafka's "topic1" topic on port 9091(broker 1's port). */
object KafkaConsumer {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "KafkaConsumer", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //(topic, message)
    val keyValueStream = stream.map(record => (record.key, record.value))
    val messages = stream.map(record => record.value)
    //print 3 messages from each rdd
    messages.foreachRDD((rdd, time) => rdd.foreach(println))

    // Kick it off
    ssc.checkpoint("tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
