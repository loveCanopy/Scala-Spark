package Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object Demo {
  
  
  def main(args: Array[String]): Unit = {
    
    
    val conf=new SparkConf().setMaster("local[2]").setAppName("streaming1")
    val ssc=new StreamingContext(conf,Seconds(10))
    
    val lines=ssc.textFileStream("E:\\TestStream\\") //¼à²âÄ³ÎÄ¼þÄ¿Â¼
    val words=lines.flatMap(_.split(" ")).map ( w => (w,1) ).reduceByKey(_+_)
    words.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  
  
}