package kafka

import java.util.Properties

import lib.Utilities._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Working example of listening for log data from Kafka's "topic1" topic on port 9091(broker 1's port). */
object KafkaProducer {

  def main(args: Array[String]) {

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
    val producer = new KafkaProducer[String, String](props)
    val topic = "topic"
    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](topic, i.toString, "Message " + i)
        val metadata = producer.send(record)
        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      producer.close()
    }

    // Kick it off
    ssc.checkpoint("tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
