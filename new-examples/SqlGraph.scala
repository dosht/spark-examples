// Some SQL
import sqlContext.implicits._
import sqlContext._
pagesWithRanksRDD.toDF().registerTempTable("Wiki")
def topNIds(n: Int) =  pagesWithRanksRDD.top(n).map(_.id)
val idList = topNIds(10).map(id => s"'$id'").reduce(_ + "," + _)
sqlContext.sql(s"SELECT * FROM Wiki WHERE id IN ($idList) ORDER BY rank DESC").show()


case class PageWithRankAndInDegrees(id: Long, title: String, rank: Double, inDegrees: Int)
(wikiGraph.inDegrees join (pagesWithRanksRDD.map(p => p.id -> p))).mapValues {
  case (inDegrees, PageWithRank(id, title, rank)) => PageWithRankAndInDegrees(id, title, rank, inDegrees)
}.values.toDF.registerTempTable("Wiki2")
sqlContext.sql(s"SELECT * FROM Wiki2 WHERE id IN ($idList) ORDER BY rank DESC").show()


// Neighbors

