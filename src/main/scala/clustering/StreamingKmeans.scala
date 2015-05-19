package clustering

import java.io.{FileReader, BufferedReader}
import java.util.Properties

import akka.actor.{Props, ReceiveTimeout, Actor}
import akka.actor.Actor.Receive
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

object KafkaProducer {

  def props(brokerList: String,
            topic: String,
            numberOfMessages: Int = -1): Props =
    Props(new KafkaProducer(brokerList, topic, numberOfMessages))
}

class KafkaProducer(
                     val brokerList: String,
                     val topic: String,
                     numberOfMessages: Int) extends Actor {
  import scala.concurrent.duration._

  val producer = startProducer()
  val reader = new BufferedReader(new FileReader("data/kddcup.data"))
  var currentCount = 0

  override def preStart(): Unit = {
    context.setReceiveTimeout(50 milliseconds)
  }

  override def receive: Receive = {
    case ReceiveTimeout if(currentCount > numberOfMessages && numberOfMessages != -1)=>
      context.setReceiveTimeout(Duration.Undefined)

    case ReceiveTimeout =>
      val line = reader.readLine()
      if(line != null) {
        val msg = new KeyedMessage[String, String](topic, "network-traffic", line)
        println(s"Sending data to ${topic}")
        producer.send(msg)
      }
  }

  private def startProducer() = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    new Producer[String, String](config)
  }
}


object StreamingKmeans {

  val brokerList = "192.168.59.103:49178,192.168.59.103:49179"
  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list" -> brokerList,
    "zookeeper.connect" -> "192.168.59.103:49177")

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[3]", "streaming-kmeans")

    val ssc = new StreamingContext(sc, Seconds(1))
    SparkEnv.get.actorSystem.actorOf(KafkaProducer.props(
      brokerList, "networklog2"))

    SparkEnv.get.actorSystem.actorOf(KafkaProducer.props(
      brokerList, "networklogtrain2"))

    val accessLogs: DStream[String] =
      KafkaUtils.
        createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
          kafkaParams,
          Set("networklog2")).map(_._2)

    val trainData: DStream[String] =
      KafkaUtils.
        createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
          kafkaParams,
          Set("networklogtrain2")).map(_._2)

    val trainVectors: DStream[Vector] = trainData.transform(rdd => toVector(rdd))
    val accessLogsVectors: DStream[Vector] = accessLogs.transform(rdd => toVector(rdd))

    val model = new StreamingKMeans()
      .setK(args(3).toInt)
      .setDecayFactor(1.0)
      .setRandomCenters(args(4).toInt, 0.0)

    model.trainOn(trainVectors)

    model.predictOn(accessLogsVectors).print()

    ssc.start()
    ssc.awaitTermination()

  }

  private def toVector(rdd: RDD[String]): RDD[Vector] = {
    import RunKMeans._
    val parseFunction = buildCategoricalAndLabelFunction(rdd)
    val originalAndData: RDD[(String, Vector)] = rdd.map(line => (line, parseFunction(line)._2))
    originalAndData.values
  }
}
