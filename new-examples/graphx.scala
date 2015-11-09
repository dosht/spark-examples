// Some imports
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

trait Vertex
case class UserVertex(name: String, job: String) extends Vertex
case class ProductVertex(name: String) extends Vertex

val users: RDD[(VertexId, UserVertex)] =
  sc.parallelize(Array((3L, UserVertex("rxin", "student")), (7L, UserVertex("jgonzal", "postdoc")),
                       (5L, UserVertex("franklin", "prof")), (2L, UserVertex("istoica", "prof"))))
// Create an RDD for edges
val relationships: RDD[Edge[ProductVertex]] =
  sc.parallelize(Array(Edge(3L, 7L, ProductVertex("collab")),    Edge(5L, 3L, ProductVertex("advisor")),
                       Edge(2L, 5L, ProductVertex("colleague")), Edge(5L, 7L, ProductVertex("pi"))))
// Define a default user in case there are relationship with missing user
val defaultUser = UserVertex("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser)

