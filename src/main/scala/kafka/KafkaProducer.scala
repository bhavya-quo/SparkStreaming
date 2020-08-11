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
    val producer = new KafkaProducer[String, String](props)
    val topic = "topic"
    try {
      for (i <- 0 to 15) {
        val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
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



//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams)
//    )






//    //(topic, message)
//    val keyValueStream = stream.map(record => (record.key, record.value))
//    val messages = stream.map(record => record.value)
//    //print 3 messages from each rdd
//    messages.foreachRDD((rdd, time) => rdd.take(3).foreach(println))

    // Kick it off
    ssc.checkpoint("tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
