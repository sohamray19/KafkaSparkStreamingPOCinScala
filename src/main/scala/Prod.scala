import java.util.Properties
import java.util.concurrent.TimeUnit

import com.exa.User
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.IntegerSerializer

object Prod {
  def main(args: Array[String]): Unit = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092")
    properties.setProperty("key.serializer", classOf[IntegerSerializer].getName)
    properties.setProperty("value.serializer", classOf[KafkaAvroSerializer].getName)
    properties.setProperty("schema.registry.url", "http://127.0.0.1:8081")
    properties.put("auto.offset.reset", "latest")

    val topic = "kspark5"
    val rand = scala.util.Random
    val producer = new KafkaProducer[Integer, User](properties)
    try {
      while (true) {

        val key: Int = 1
        val user = User.newBuilder().setField1(rand.nextBoolean().toString).build()
        val pr = new ProducerRecord[Integer, User](topic, key, user)
        val metadata = producer.send(pr).get()
        println(metadata)
        TimeUnit.SECONDS.sleep(rand.nextInt(10))
      }
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    producer.flush()
    producer.close()
  }
}