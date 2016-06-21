package Streaming
//STATEFUL 
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel

object Demo3 {
  
  
  def main(args: Array[String]): Unit = {
    
    if (args.length != 2) {
      System.err.println("Usage: StatefulWordCount <filename> <port> ")
      System.exit(1)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
 
    // �������״̬����������valuesΪ��ǰ���ε���Ƶ�ȣ�stateΪ�������ε���Ƶ��
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
 
    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
 
    // ����StreamingContext��Spark Steaming����ʱ����Ϊ5��
    val ssc = new StreamingContext(sc, Seconds(5))
    // ����checkpointĿ¼Ϊ��ǰĿ¼
    ssc.checkpoint("E:\\TestStream\\")  
 
    // ��ȡ��Socket���͹�������
    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map(x => (x, 1))
 
    // ʹ��updateStateByKey������״̬��ͳ�ƴ����п�ʼ���������ܵĴ���
    val stateDstream = wordCounts.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
    
    
    
 
  
}