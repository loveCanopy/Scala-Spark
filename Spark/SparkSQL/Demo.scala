package Test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
case class Person(date:String,session:String,name:String,c_seq:Int,s_seq:Int,url:String)
object Demo {
  
  def main(args: Array[String]): Unit = {
    
  val conf=new SparkConf().setMaster("local").setAppName("Spark-SQL")
  val sc=new SparkContext(conf)
  
  val sqlcontext=new org.apache.spark.sql.SQLContext(sc)
  import sqlcontext.implicits._
 
  
  val data=sc.textFile("e:\\mllib\\SogouQ1.txt").map{_.split("\t")}.map(s=>Person(s(0),s(1),s(2),s(3).trim().toInt,s(4).trim().toInt,s(5))).toDF()
  
  data.registerTempTable("sogoudata")
  val logs=sqlcontext.sql("select * from sogoudata limit 10")
  logs.foreach { println }
  

  
  
  
  //json
//  val df = sqlContext.read.json("examples/src/main/resources/people.json")
//
////�򵥲���
//df.show()
//df.printSchema()  // Print the schema in a tree format
//df.select("name").show() 
//df.filter(df("age") > 21).show()  // Select people older than 21 
//df.select(df("name"), df("age") + 1).show()  // Select everybody, but increment the age by 1 
//df.groupBy("age").count().show()  // Count people by age
//
////ע����ʱ��
//df.registerTempTable("df")
//
////�Ա���в���
//val people=sqlContext.sql("sql���")
//people.show
//  
//  
  
  
  
//  hive��
//  sqlcontext.sql("HQL���")
  
//  Parquet�ļ�
//  people.write.parquet("people.parquet")
//  val parquetFile = sqlContext.read.parquet("people.parquet")
//parquetFile.registerTempTable("parquetFile")
  
  }
  
  
  
}