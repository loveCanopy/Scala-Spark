package CF
 //��Ʒ�Ƽ� ����һ����Ʒ������Щ��Ʒ�������� 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

import org.jblas.DoubleMatrix

object Item {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("CF-item")
    val sc=new SparkContext(conf)
    val rawData=sc.textFile("E:\\mllib\\ml-100k\\ml-100k\\u.data", 1)
    val rawRatings=rawData.map { _.split("\t").take(3) }
    
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    
    //һ����ratingģ�͹��ɵ�RDD
    val ratings=rawRatings.map { case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble) }
    //����һ��MaxrixFactorizationModel���� ���û����Ӻ���Ʒ���ӱ����ڣ�id,factor��RDD��
    val model=ALS.train(ratings, 50, 10,0.01)
    
    //ʹ���������ƶ����ⶨ���ƶ�
    val aMatrix=new DoubleMatrix(Array(1.0,2.0,3.0))
    
    //���庯��������������֮����������ƶ�
    def cos(vec1:DoubleMatrix,vec2:DoubleMatrix):Double={
      vec1.dot(vec2)/(vec1.norm2()*vec2.norm2())
    }
    val itemID=567
    val itemFactor=model.productFeatures.lookup(itemID).head
    val itemVector=new DoubleMatrix(itemFactor)
    cos(itemVector,itemVector)
    
    //�������Ʒ�����ƶ�
    val sims=model.productFeatures.map{  case (id,factor)=>
       val factorVector=new DoubleMatrix(factor)
       val sim=cos(factorVector,itemVector)
       (id,sim)
    
    }
    
    
    val K=10
    val sortedSims=sims.top(K)(Ordering.by[(Int,Double),Double]{
      case(id,similarity)
     => similarity} )
   
     println(sortedSims.take(10).mkString("\n"))
     
     //����Ƽ���������Ʒ
      //�����Ƽ�����
    val movies=sc.textFile("E:\\mllib\\ml-100k\\ml-100k\\u.item", 1)
    val titles=movies.map { line => line.split("\\|").take(2) }.map { array => (array(0).toInt,array(1) )}.collectAsMap()
     val sortedSim=sims.top(K+1)(Ordering.by[(Int,Double),Double] { case (id,similarity) => similarity })
     sortedSim.slice(1, 11).map{case (id,sim)=>(titles(id),sim)}.mkString("\n").foreach { print }
     
     
  }
}