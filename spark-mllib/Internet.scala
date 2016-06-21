package Kmeans

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.rdd.RDD
 
object Internet {
  
  def main(args: Array[String]): Unit = {
  val conf=new SparkConf().setMaster("local").setAppName("K-MEANS")
  val sc=new SparkContext(conf)
  val rawData=sc.textFile("E:\\mllib\\kddcup.data\\kddcup.data_10_percent_corrected")
  val data=rawData.map {_.split(",").last}.countByValue().toSeq.sortBy(_._2).reverse
  //输出(smurf.,2807886)
//(neptune.,1072017)
//(normal.,972781)
//(satan.,15892)
//(ipsweep.,12481)
  
  //除去类别型列和最后的标号列
  val labelsAndData=rawData.map{
    line =>
      val buffer=line.split(",").toBuffer
      buffer.remove(1,3)
      val label=buffer.remove(buffer.length-1)
      val vector=Vectors.dense(buffer.map ( _.toDouble ).toArray)
      (label,vector)
    
  }
  //缓存
   val new_data=labelsAndData.values.cache()
   val kmeans=new KMeans()
   val model=kmeans.run(new_data)
   model.clusterCenters.foreach(println)
   
   val clusterlabelCount=labelsAndData.map{
     case(label,datum)=>
       val cluster=model.predict(datum)
       (cluster,label)
   }.countByValue
   
   clusterlabelCount.toSeq.sorted.foreach{
     
     case((cluster,label),count)=>
       println(cluster+" "+label+" "+count)
     
     
   }
  
//  }
// 
//  
   
  
   //欧式距离函数
    def distance(a:Vector,b:Vector)={
       
      math.sqrt(a.toArray.zip(b.toArray).map(p=>p._1-p._2).map(d=>d*d).sum)
    }
   
   
   
   def disttoCentroid(datum:Vector ,model :KMeansModel)={
      
      val cluster=model.predict(datum)
      val centroid=model.clusterCenters(cluster)
      distance(centroid,datum)
      
    }
   
   def clusteringScore(data:RDD[Vector],k:Int)={
     
     val kmeans=new KMeans()
     kmeans.setK(k)
     val model=kmeans.run(data)
     data.map(datum=>disttoCentroid(datum,model)).mean()
     
   }
   
   //(5 to 40 by 5).map(k=>(k,clusteringScore(new_data,k))).foreach(println)
   
   
   val dataAsArray=new_data.map { _.toArray }
   val numcols=dataAsArray.first().length //数组长度
   val n=dataAsArray.count() //总记录数 
   val sums=dataAsArray.reduce((a,b)=>a.zip(b).map(t=>t._1+t._2)) //向量之和
   val means=sums.map { _/n } //各个维度的平均值
   
   val sumSquares=dataAsArray.fold(new Array[Double](numcols))((a,b)=>a.zip(b).map(t=>t._1+t._2*t._2)) //平方和返回一个数组
   
   val stdevs=sumSquares.zip(sums).map{                 //标准差
     case(sumSq,sum)=>math.sqrt(n*sumSq-sum*sum)/n
     
     
   }
   
   //标准化函数
   def normalize(data:Vector)={
     val normalizedArray=(data.toArray,means,stdevs).zipped.map(
     (value,mean,stdev)=>
       if(stdev<=0) (value-mean) else (value-mean)/stdev
     
     )
     
     Vectors.dense(normalizedArray)
   }
   
   
   val normalizedData=new_data.map (normalize).cache()
   (60 to 120 by 10).par.map(k=>(k,clusteringScore(normalizedData,k))).toList.foreach(println)
   
  }
  
}