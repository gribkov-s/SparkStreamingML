import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.csv.{CSVFormat, CSVParser, CSVRecord}

import scala.util.Try
import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import java.time.LocalDateTime
import scala.collection.JavaConverters.asScalaBufferConverter


object ProducerForInputData extends App {

  implicit val formats = DefaultFormats

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val path = "src/main/resources/iris-1801-dbdda0.csv"
  val reader = Files.newBufferedReader(Paths.get(path))
  val csvParser = new CSVParser(reader, CSVFormat.RFC4180)
  val records = csvParser.getRecords

  records.asScala.foreach(rec => {
    val iris = Try {
      IrisNoClass(
        rec.get(0).toDouble,
        rec.get(1).toDouble,
        rec.get(2).toDouble,
        rec.get(3).toDouble
      )
    }

    if (iris.isSuccess) {
      val key = LocalDateTime.now().toString
      val value = write(iris.get).toString
      val record = new ProducerRecord[String, String]("iris_no_class_topic", key, value)
      producer.send(record)
    }
  })

  producer.close()
}
