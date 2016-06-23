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
    
    //graph.vertices ����һ�� VertexRDD[(�ַ���,�ַ���)] ���� ����((VertexID(String,String))) ��������ʹ��scala case ���ʽ���⹹ Ԫ�顣
    //��һ����, graph.edges ����һ�� EdgeRDD ���� ��Ե(�ַ���) ����
    graph.vertices.filter{case (id,(name,pos))=>pos=="postdoc"}.count()
    
    graph.edges.filter { e => e.srcId>e.dstId }.count()
    graph.edges.filter { case Edge(src,dst,prop) => src>dst }.count
    val facts: RDD[String] =
  graph.triplets.map(triplet =>
    triplet.srcAttr._1+triplet.srcAttr._2+ " is the " + triplet.attr + " of " + triplet.dstAttr._1+triplet.dstAttr._2)
//facts.collect.foreach(println(_))




//ͼ����Ϣ
 graph.numEdges   //��
 graph.numVertices //��
 graph.inDegrees //���
 graph.outDegrees //����
 graph.degrees  //��
 
 
//ͼ�ļ���
 
 graph.vertices 
 graph.edges
 graph.triplets
//����
 graph.persist(StorageLevel.MEMORY_ONLY)
 graph.cache()
 graph.unpersist()
 graph.unpersistVertices()
 //�ı�ߺͶ��������
 val new_ver=graph.vertices.map{case (id,(name,pos))=>(id,name,pos)}
 //new_ver.foreach(println)
 //�ȼ���graph.mapVertices(((id,(name,pos))=>(id,name,pos)))

 //�ṹ����
 //reverse subgraph mask groupEdges
 val validgraph=graph.subgraph(vpred=(id,attr)=>attr._2!="postdoc")
 //validgraph.vertices.collect().foreach(println)
 
 val ccGraph=graph.connectedComponents()
 val vallidCCGraph=ccGraph.mask(validgraph)
 //vallidCCGraph.vertices.collect().foreach(println)
 
 //join����
 val outdegrees: VertexRDD[Int]=graph.outDegrees //����
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