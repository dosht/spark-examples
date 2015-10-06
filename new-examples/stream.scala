// Import and init the streaming context
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds

val ssc = new StreamingContext(sc, Seconds(1))
ssc.checkpoint(".")

// Processing one stream - Stateless
val lines = ssc.socketTextStream("localhost", 7777)
lines.map(l => l -> l.toInt * 8).print()
lines.flatMap(_.split(" ")).print()

// Processing 2 streamins
val lines2 = ssc.socketTextStream("localhost", 9999)
(lines.flatMap(_.split(" ")) union lines2).map(_.toInt).map(i => i -> i * 8).print()

// Ptocessing with TimeWindow - Stateful
import scala.util.Try
def safeToInt(x: String): Option[Int] = try {Some(x.toInt)} catch {case e => None}

lines
.flatMap(safeToInt(_))
.reduceByWindow(
  (x, y) => if(y % 2 == 0) x + y else y,
  (x, y) => x - y,
  Seconds(3),
  Seconds(1))
.print()

(new Thread {
  override def run() {
    ssc.start()
    ssc.awaitTermination()
  }
}).start()

ssc.stop()

