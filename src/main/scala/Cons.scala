import com.exa.User
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils}


object Cons {
  def main(args: Array[String]) {

    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("breeze").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9092",
      "key.deserializer" -> classOf[IntegerDeserializer].getName,
      "value.deserializer" -> classOf[KafkaAvroDeserializer].getName,
      "schema.registry.url" -> "http://127.0.0.1:8081",
      "group.id" -> "customer-group1",
      "specific.avro.reader" -> "true",
      "auto.offset.reset" -> "latest"
    )

    val topics = List("kspark5")
    val sparkConf = new SparkConf().setAppName("KSS").setMaster("local[*]")
    //  create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val stream = KafkaUtils.createDirectStream[Integer, User](
      ssc,
      PreferBrokers,
      ConsumerStrategies.Subscribe[Integer, User](topics, kafkaParams)
    )

    //    stream.foreachRDD { rdd =>
    //      rdd.foreach(record => {
    //        val value = record.key()
    //        println(value)
    //      })
    //    }

    var newstream = stream.map(record => (record.value.getField1, record.key().toInt))
    var wordcount = newstream.reduceByKeyAndWindow(_ + _, Durations.seconds(180))
    wordcount.foreachRDD(rdd => rdd.foreach(println))

    ssc.start()
    ssc.awaitTermination()
  }
}