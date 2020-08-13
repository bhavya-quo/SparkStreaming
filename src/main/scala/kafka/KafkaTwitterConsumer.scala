package kafka

import lib.Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object KafkaTwitterConsumer {

  case class Tweet(id:Long, text:String, timestamp:Long)
  def mapper(line:String): Tweet = {
    val fields = line.split(',')

    val person:Tweet = Tweet(fields(0).toLong, fields(1).toString, fields(2).toLong)
    person
  }

  def main(args: Array[String]) {

    val spark = SparkSession.builder().master("local[*]").appName("KafkaConsumer").getOrCreate()

    setupLogging()

//    import spark.implicits._

    import org.apache.spark.sql.types._

    val lines = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      .option("group.id","twitter")
      .option("auto.offset.reset","latest")
      .option("enable.auto.commit",false: java.lang.Boolean)
      .option("subscribe","twitter")
      .load()

    val df = lines.selectExpr("cast (value as String) as json")

    df.printSchema()

    val schema = new StructType()
      .add("id",StringType, nullable = false)
      .add("hashtags",ArrayType(StringType), nullable = false)
      .add("text",StringType, nullable = false)
      .add("timestamp",LongType, nullable = false)

    val df1 = df.select(from_json(col("json"), schema).as("tweets")).select("tweets.*")

    val df2 = df1.select(explode(col("hashtags"))).as("hashTagsArr")

    val df3 = df2.select("col").groupBy("col").count().sort(desc("count"))

    val query = df3.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .start()

    query.awaitTermination()

  }
}
