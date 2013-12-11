/*** SimpleApp.scala ***/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.scheduler.SplitInfo

object SimpleApp extends App {
  val sparkHome = Option(System getenv "SPARK_HOME") getOrElse "../incubator-spark/"
  val jarFile = "target/scala-2.10/spark-first_2.10-0.1-SNAPSHOT.jar"

  val logFile = s"$sparkHome/README.md" // Should be some file on your system
  val sc = new SparkContext("local", "Simple App", sparkHome, List(jarFile), Map(), Map())

  val logData = sc.textFile(logFile, 2).cache()
  val numAs = logData.filter(line => line.contains("a")).count()
  val numBs = logData.filter(line => line.contains("b")).count()
  println(Console.CYAN)
  println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  println(Console.RESET)

  sc.stop()
}