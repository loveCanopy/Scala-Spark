package CF
//�û��Ƽ� ��������û��Ƽ���Ʒ ͨ��ģ������û�����ϲ�õ�ǰK����Ʒ
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.jblas.DoubleMatrix

object Demo {
  
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setMaster("local").setAppName("CF-user")
    val sc=new SparkContext(conf)
    val rawData=sc.textFile("E:\\mllib\\ml-100k\\ml-100k\\u.data", 1)
    val rawRatings=rawData.map { _.split("\t").take(3) }
    
    import org.apache.spark.mllib.recommendation.ALS
    import org.apache.spark.mllib.recommendation.Rating
    
    //һ����ratingģ�͹��ɵ�RDD
    val ratings=rawRatings.map { case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble) }
    //����һ��MaxrixFactorizationModel���� ���û����Ӻ���Ʒ���ӱ����ڣ�id,factor��RDD��
    val model=ALS.train(ratings, 50, 10,0.01)
    
    //ʹ���Ƽ�ģ��
    //Ԥ��789�û���123��Ӱ������
    val predictedRating=model.predict(789, 123)
    
    //��789�û��Ƽ�ǰ10����Ʒ
    val userId=789
    val K=10
    val topKrecs=model.recommendProducts(userId, K)
    print(topKrecs.mkString("\t"))
    
    //�����Ƽ�����
    val movies=sc.textFile("E:\\mllib\\ml-100k\\ml-100k\\u.item", 1)
    val titles=movies.map { line => line.split("\\|").take(2) }.map { array => (array(0).toInt,array(1) )}.collectAsMap()
    
    val moviesForUser=ratings.keyBy { _.user }.lookup(789)
    println(moviesForUser.size)
    
    moviesForUser.sortBy(-_.rating).take(10).map { rating => (titles(rating.product),rating.rating) }.foreach(println)
    
    println("=======")
    //�Ա�
    topKrecs.map { rating => (titles(rating.product),rating.rating) }.foreach(println)
    
    
     //�Ƽ�ģ��Ч��������
    //������
    val userProducts=ratings.map { case Rating(user,product,rating) => (user,product) }
    val predictions=model.predict(userProducts).map { case Rating(user,product,rating)  => ((user,product),rating) }
    
    val ratingAndprodict=ratings.map { case Rating(user,product,rating)  =>  ((user,product),rating)}.join(predictions)
//    val MSE=ratingAndprodict.map{case ((user,product),(actual,predicted))=>math.pow((actual-predicted),2)}.reduce(_+_)/ratingAndprodict.count
//    println(MSE)
//    
//    val RMSE=math.sqrt(MSE)
//    println(RMSE)
    
    import org.apache.spark.mllib.evaluation.RegressionMetrics
    
    val predictANDtrue=ratingAndprodict.map{case ((user,product),(actual,predicted))=>(predicted,actual)}
    //��Ҫһ����ֵ�����͵�RDD ��һ����Ԥ��ֵ��ʵ��ֵ���ļ���
    val regressionMetrics=new RegressionMetrics(predictANDtrue)
    
    println(regressionMetrics.meanSquaredError)
    println(regressionMetrics.rootMeanSquaredError)
    
    
    
    
    
    //MAPK
    //�õ���Ʒ��������
    val itemFactors=model.productFeatures.map{ case(id,factor)=>factor}.collect()
    val itemMatrix=new DoubleMatrix(itemFactors)
    val imBroadcast=sc.broadcast(itemMatrix)
    
    //�õ��û���������
    val allRecs=model.userFeatures.map{case(id,array)=>
       val userVector=new DoubleMatrix(array)
       val scores=imBroadcast.value.mmul(userVector)//�û����Ӻ͵�Ӱ���Ӿ���˻� 
       val sortedWithid=scores.data.zipWithIndex.sortBy(-_._1)
       val recommenedIds=sortedWithid.map(_._2+1).toSeq
       (userId,recommenedIds) //�û�ID ,��ӰID
    }
    
    
    
//    
//        //APKʵ�ִ���
//    def avgPrecesionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
//      val predK=predicted.take(k)
//      var score=0.0
//      var numHits=0.0
//      for((p,i)<-predK.zipWithIndex){
//        if(actual.contains(p)){
//          numHits+=1.0
//          score=numHits/(i.toDouble+1.0)
//          
//        }
//        
//        
//      }
//      if(actual.isEmpty){
//        1.0
//      }else{
//        score/scala.math.min(actual.size, k).toDouble
//      }
//      
//    }
//    
    val userMovies=ratings.map { case Rating(user,product,rating) => (user,product) }.groupBy(_._1)
    val predictedANDTrueForRanking=allRecs.join(userMovies).map{
      case (userId,(predicted,actuallwithid))=>
        val autual=actuallwithid.map(_._2)
        (predicted.toArray,autual.toArray)
    
    }
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val rankingMetrics=new RankingMetrics(predictedANDTrueForRanking)
    println(rankingMetrics.meanAveragePrecision)
//    val n=10
//    val MAPK=allRecs.join(userMovies).map{
//      case (userI,(predicted,actuallwithid))=>
//        val autual=actuallwithid.map(_._2).toSeq
//        avgPrecesionK(autual,predicted,n)
//     
//    }.reduce(_+_)/allRecs.count()
    
//    println(MAPK)
//    //MAPK
//    //789�û����۹��ĵ�Ӱ
//    val actuals=moviesForUser.map { _.product }
//    //�Ƽ���ǰ10����Ӱ
//    val predi=topKrecs.map { _ .product }
//    //APKʵ�ִ���
//    def avgPrecesionK(actual:Seq[Int],predicted:Seq[Int],k:Int):Double={
//      val predK=predicted.take(k)
//      var score=0.0
//      var numHits=0.0
//      for((p,i)<-predK.zipWithIndex){
//        if(actual.contains(p)){
//          numHits+=1.0
//          score=numHits/(i.toDouble+1.0)
//          
//        }
//        
//        
//      }
//      if(actual.isEmpty){
//        1.0
//      }else{
//        score/scala.math.min(actual.size, k).toDouble
//      }
//      
//    }
    
//    val apk10=avgPrecesionK(actuals, predi, 10)
//    
//    println(apk10)
//  }
//  
 
  }
  
}