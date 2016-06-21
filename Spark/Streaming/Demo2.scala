package Streaming
//流数据接受器
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.storage.StorageLevel

object Demo2 {
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[2]").setAppName("socket")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))   
    val lines=ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words=lines.flatMap(_.split(",")).map ( w => (w,1) ).reduceByKey(_+_)
    words.print()
    ssc.start()
    ssc.awaitTermination()
    
  }
  
  
  
}