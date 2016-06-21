package Streaming
//流数据模拟器
import scala.io.Source
import java.net.ServerSocket
import java.io.PrintWriter

object Demo1 {
  //得到长度内的随机数
  def index(length:Int)={
    import java.util.Random
    val ran=new Random
    ran.nextInt(length)
  }
  

  def main(args: Array[String]): Unit = {
      if(args.length!=3){
        System.err.println("<filename><port><mill>")
        System.exit(1)
      }
          
    val filename=args(0)
    val lines=Source.fromFile(filename).getLines().toList
    val filerow=lines.length
 
    val listener=new ServerSocket(args(1).toInt) //socket端口号
    
    while(true){
      val socket=listener.accept() //有人监听
      new Thread(){
        override def run={
          println("from :"+socket.getInetAddress)
          val out=new PrintWriter(socket.getOutputStream(),true)
          while(true){
            
            Thread.sleep(args(2).toLong)
            val content=lines(index(filerow))
            println(content)
            out.write(content+"\n")
            out.flush()
          }
          
          socket.close()
          
        }
        
        
      }.start()
      
      
      
      
    }
    
    
    
  }
}