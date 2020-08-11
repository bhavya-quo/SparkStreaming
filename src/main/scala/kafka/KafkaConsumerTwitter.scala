package kafka

import org.apache.spark.streaming.{Seconds, StreamingContext}
import lib.Utilities._
import org.apache.spark.sql.functions._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.parquet.format.IntType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/** Working example of listening for log data from Kafka's "topic1" topic on port 9091(broker 1's port). */
object KafkaConsumerTwitter {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val spark = SparkSession.builder().master("local[*]").appName("KafkaConsumer").getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(1))

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

    val topics = Array("twitter")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    //(topic, message)
    val tweets = stream.map(record => (record.key, record.value))
    tweets.foreachRDD(rdd => {
      import spark.implicits._
      val df = rdd.toDF()

        df.selectExpr("CAST(_1 AS STRING)","CAST(_2 AS STRING)").show()

      rdd.collect().foreach(tweet => {
        println("Time: " + tweet._1 + ", Tweet: " + tweet._2)
    })


    })
    // Kick it off
    ssc.checkpoint("tmp/checkpoint")
    ssc.start()
    ssc.awaitTermination()
  }
}
