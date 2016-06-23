package Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.Graph
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.VertexRDD

object Message {
  
  
  def main(args: Array[String]): Unit = {
    
      val sparkconf=new SparkConf().setMaster("local[2]").setAppName("GRAPHX1")
    val sparkcontext=new SparkContext(sparkconf)
   
    val graph: Graph[Double, Int] =
  GraphGenerators.logNormalGraph(sparkcontext, numVertices = 100).mapVertices( (id, _) => id.toDouble )
// Compute the number of older followers and their total age
val olderFollowers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](
  triplet => { // Map Function
    if (triplet.srcAttr > triplet.dstAttr) {
      // Send message to destination vertex containing counter and age
      triplet.sendToDst(1, triplet.srcAttr)
    }
  },
  // Add counter and age
  (a, b) => (a._1 + b._1, a._2 + b._2) // Reduce Function
)
// Divide total age by number of older followers to get average age of older followers
val avgAgeOfOlderFollowers: VertexRDD[Double] =
  olderFollowers.mapValues( (id, value) => value match { case (count, totalAge) => totalAge / count } )
// Display the results
avgAgeOfOlderFollowers.collect.foreach(println(_))
  }
  
  
}