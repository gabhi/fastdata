initialCommands += """
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  val sc = new SparkContext("local", "Intro")
  """

cleanupCommands += """
  println("Closing the SparkContext:")
  sc.stop()
  """

libraryDependencies ++= Seq(
  "com.typesafe.play" %% "play-json" % "2.3.0"
//  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0-M4"
)

