package Streaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
object Demo4 {
  
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: WindowWorldCount <filename> <port> <windowDuration> <slideDuration>")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
 
     // ����StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
     // ����checkpointĿ¼Ϊ��ǰĿ¼
    ssc.checkpoint(".")
 
    // ͨ��Socket��ȡ���ݣ��ô���Ҫ�ṩSocket���������Ͷ˿ںţ����ݱ������ڴ��Ӳ����
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(","))
 
    // windows��������һ�ַ�ʽΪ���Ӵ����ڶ��ַ�ʽΪ��������                        //windows��ʱ������С(30s ������RDDΪһ��windows)   windows�����ƶ���ʱ������10s�ƶ�һ�Σ�ÿ�и����Σ��ƶ�һ�Σ�   ����Ϊstreamingtext�ı���
    val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(args(2).toInt), Seconds(args(3).toInt))
//val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow(_+_, _-_,Seconds(args(2).toInt), Seconds(args(3).toInt))
 
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
  
  
  
}