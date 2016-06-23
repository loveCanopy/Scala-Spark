package Graph
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Demo {

  
  def main(args: Array[String]): Unit = {
    
    val sparkconf=new SparkConf().setMaster("local[2]").setAppName("GRAPHX")
    val sparkcontext=new SparkContext(sparkconf)
    val users:RDD[(VertexId,(String,String))]=sparkcontext.parallelize(Array((3L,("rxin","student")),(7L,("jgonzal","postdoc")),(5L,("franklin","prof")),(2L,("istoica","prof"))))
    val relationship:RDD[Edge[String]]=sparkcontext.parallelize(Array(Edge(3L,7L,"collab"),Edge(5L,3L,"advisor"),Edge(2L,5L,"colleague"),Edge(5L,7L,"pi")))
    val defaultUser=("John Doe","Missing")
    val graph=Graph(users,relationship,defaultUser)
    
    //graph.vertices 返回一个 VertexRDD[(字符串,字符串)] 延伸 抽样((VertexID(String,String))) 所以我们使用scala case 表达式来解构 元组。
    //另一方面, graph.edges 返回一个 EdgeRDD 包含 边缘(字符串) 对象。
    graph.vertices.filter{case (id,(name,pos))=>pos=="postdoc"}.count()
    
    graph.edges.filter { e => e.srcId>e.dstId }.count()
    graph.edges.filter { case Edge(src,dst,prop) => src>dst }.count
    val facts: RDD[String] =
  graph.triplets.map(triplet =>
    triplet.srcAttr._1+triplet.srcAttr._2+ " is the " + triplet.attr + " of " + triplet.dstAttr._1+triplet.dstAttr._2)
//facts.collect.foreach(println(_))




//图的信息
 graph.numEdges   //点
 graph.numVertices //边
 graph.inDegrees //入度
 graph.outDegrees //出度
 graph.degrees  //度
 
 
//图的集合
 
 graph.vertices 
 graph.edges
 graph.triplets
//缓存
 graph.persist(StorageLevel.MEMORY_ONLY)
 graph.cache()
 graph.unpersist()
 graph.unpersistVertices()
 //改变边和顶点的属性
 val new_ver=graph.vertices.map{case (id,(name,pos))=>(id,name,pos)}
 //new_ver.foreach(println)
 //等价于graph.mapVertices(((id,(name,pos))=>(id,name,pos)))

 //结构操作
 //reverse subgraph mask groupEdges
 val validgraph=graph.subgraph(vpred=(id,attr)=>attr._2!="postdoc")
 //validgraph.vertices.collect().foreach(println)
 
 val ccGraph=graph.connectedComponents()
 val vallidCCGraph=ccGraph.mask(validgraph)
 //vallidCCGraph.vertices.collect().foreach(println)
 
 //join操作
 val outdegrees: VertexRDD[Int]=graph.outDegrees //出度
 outdegrees.foreach(println)
 val degreeGraph=graph.outerJoinVertices(outdegrees){
   
    (id, oldAttr, outDegOpt) =>
  outDegOpt match {
    case Some(outDeg) => outDeg
    case None => 0 // No outDegree means zero outDegree
  }
   
   
 }
 
  degreeGraph.edges.foreach { println}
 
 
  }
  
  
   
  
    

  

  
}