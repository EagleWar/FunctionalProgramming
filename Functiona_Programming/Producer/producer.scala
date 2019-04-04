import java.util
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.collection.JavaConversions._
import scala.io.Source


object ScalaProducerExample extends App {


  val producerProperties = new Properties()
  producerProperties.put("bootstrap.servers", "127.0.0.1:9092")
  producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](producerProperties)

  val lines = Source.fromFile("data.txt").getLines.toList.map(x=>{  // we read the txt file which contains our data about drones
      Thread.sleep(2000)
      val record = new ProducerRecord[String, String]("iot" , x) //we produce in the topic iot all kind of data (normal and danger)
      producer.send(record)
    }
  )

  producer.close()
}
