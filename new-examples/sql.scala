// import and init sqlContext
import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._

// Load some json
val path = "./MOCK_DATA.json"
val df = sqlContext.read.json(path)
df.show()

// print the schema
df.printSchema()

// data frame select
df.select("firstName").show()

df.registerTempTable("data")
val selection = sqlContext.sql("SELECT * FROM data WHERE firstName = 'Amy'")
import org.apache.spark.sql.Row
case class Person(firstName: String, lastName: String, country: String, email: String, ipAddress: String)
def toPerson(row: Row) = Person(row.getString(3), row.getString(6), row getString 1, row getString 2, row getString 5)
selection.map(toPerson).collect()

val morePersons = sc.parallelize(List(Person("Mostafa", "Abdulhamid", "Egypt", "mosafa@cake.net", "127.0.0.1")))
  .union(selection map toPerson)
morePersons.toDF().registerTempTable("Persons")

sqlContext.sql("SELECT firstName, lastName from Persons").show()


val x = selection.collect()(0)

