package CF
//用户推荐 向给定的用户推荐物品 通过模型求出用户可能喜好的前K个商品
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
    
    //一个由rating模型构成的RDD
    val ratings=rawRatings.map { case Array(user,movie,rating) => Rating(user.toInt,movie.toInt,rating.toDouble) }
    //返回一个MaxrixFactorizationModel对象 ，用户因子和物品因子保存在（id,factor）RDD中
    val model=ALS.train(ratings, 50, 10,0.01)
    
    //使用推荐模型
    //预测789用户对123电影的评分
    val predictedRating=model.predict(789, 123)
    
    //给789用户推荐前10个物品
    val userId=789
    val K=10
    val topKrecs=model.recommendProducts(userId, K)
    print(topKrecs.mkString("\t"))
    
    //检验推荐内容
    val movies=sc.textFile("E:\\mllib\\ml-100k\\ml-100k\\u.item", 1)
    val titles=movies.map { line => line.split("\\|").take(2) }.map { array => (array(0).toInt,array(1) )}.collectAsMap()
    
    val moviesForUser=ratings.keyBy { _.user }.lookup(789)
    println(moviesForUser.size)
    
    moviesForUser.sortBy(-_.rating).take(10).map { rating => (titles(rating.product),rating.rating) }.foreach(println)
    
    println("=======")
    //对比
    topKrecs.map { rating => (titles(rating.product),rating.rating) }.foreach(println)
    
    
     //推荐模型效果的评估
    //均方差
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
    //需要一个键值对类型的RDD ，一个（预测值，实际值）的集合
    val regressionMetrics=new RegressionMetrics(predictANDtrue)
    
    println(regressionMetrics.meanSquaredError)
    println(regressionMetrics.rootMeanSquaredError)
    
    
    
    
    
    //MAPK
    //得到物品因子向量
    val itemFactors=model.productFeatures.map{ case(id,factor)=>factor}.collect()
    val itemMatrix=new DoubleMatrix(itemFactors)
    val imBroadcast=sc.broadcast(itemMatrix)
    
    //得到用户因子向量
    val allRecs=model.userFeatures.map{case(id,array)=>
       val userVector=new DoubleMatrix(array)
       val scores=imBroadcast.value.mmul(userVector)//用户因子和电影因子矩阵乘积 
       val sortedWithid=scores.data.zipWithIndex.sortBy(-_._1)
       val recommenedIds=sortedWithid.map(_._2+1).toSeq
       (userId,recommenedIds) //用户ID ,电影ID
    }
    
    
    
//    
//        //APK实现代码
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
//    //789用户评论过的电影
//    val actuals=moviesForUser.map { _.product }
//    //推荐的前10个电影
//    val predi=topKrecs.map { _ .product }
//    //APK实现代码
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