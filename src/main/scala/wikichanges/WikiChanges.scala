package wikichanges

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Milliseconds, Seconds, StreamingContext}
import play.api.libs.json.Json
import org.apache.spark.streaming.StreamingContext._

object WikiChanges {


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[3]", "Intro")

    def createContext(): StreamingContext = {
      val ssc = new StreamingContext(sc, Seconds(1))
      ssc.checkpoint("data/checkpoint")
      val wikiChanges = ssc.socketTextStream("localhost", 8124)
      val urlAndCount: DStream[(String, Int)] = wikiChanges
        .flatMap(_.split("\n"))
        .map(Json.parse(_))
        .map(j => (j \ "pageUrl").as[String] -> 1)

      val topEdits = urlAndCount.reduceByKeyAndWindow(
        reduceFunc = _ + _,
        invReduceFunc = _ - _,
        windowDuration = Seconds(5 * 60),
        filterFunc = _._2 > 5)
        .transform(_.sortBy(_._2))

      //topEdits.foreachRDD(rdd => rdd.foreach(println))

      topEdits.print()
      ssc
    }

    val ssc = StreamingContext.getOrCreate("data/checkout", createContext _)

    ssc.start()
    ssc.awaitTermination()
    
    ssc.stop(stopSparkContext = true)
  }
}
