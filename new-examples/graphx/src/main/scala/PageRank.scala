import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.math.Ordering

case class PageWithRank(id: Long, title: String, rank: Double)

object PageWithRank {
  implicit val pageOrdering = Ordering.by[PageWithRank, Double](_.rank)
}

object PageRank {
  def pageHash(title: String): Long = title.##

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf(false).setMaster("local[*]").setAppName("PageRand-GraphX")
    val sc = new SparkContext(conf)

    val path = "graphx/links.tsv"
    val input = sc.textFile(path).filter(!_.startsWith("#")).map(_.split("\t")).cache()

    val pages = input.flatMap(x => x).distinct.map(title => pageHash(title) -> title)
    val links = input.flatMap {
      case a@ Array(x, y) => Some(Edge(pageHash(x), pageHash(y), a))
      case _ => None
    }
    val wikiGraph = Graph(pages, links, "")

    val pageRankRDD = wikiGraph.pageRank(tol = 0.0001).vertices.cache() // tol: the tolerance allowed at convergence (smaller => more accurate).
    val pagesWithRanksRDD: RDD[PageWithRank] = (pages join pageRankRDD) map {
      case (id, (title, rank)) => PageWithRank(id, title, rank)
    }

    println("=== Result: ========================")
    pagesWithRanksRDD.top(10).foreach(println)
    println("====================================")
    sc.stop()
  }
}









