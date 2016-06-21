package Demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import scala.Tuple2;
import scala.Tuple3;

public class wordcount_line {
      @org.junit.Test 
	public  void TestLine() {
		// TODO Auto-generated method stub

		SparkConf conf=new SparkConf().setMaster("local").setAppName("wordcountbyline");
		JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("E:\\ScalaTest\\d.txt",1);
        JavaPairRDD<String, Integer> mapToPair = textFile.mapToPair(new PairFunction<String,String,Integer>(){

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0,1);
			}
        	
        	
        });
		JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer,Integer,Integer>(){

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
			
		});
        
		reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>(){

			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
			
			
		});
	}
     
      
      
      public static void main(String[] args){
    	  SparkConf conf=new SparkConf().setMaster("local").setAppName("wordcountbyline");
  		  JavaSparkContext sc=new JavaSparkContext(conf);
          JavaRDD<String> textFile = sc.textFile("E:\\ScalaTest\\d.txt",1);
          
          
 
          JavaPairRDD<Integer, String> mapToPair = textFile.mapToPair(new PairFunction<String,Integer,String>(){

			@Override
			public Tuple2<Integer, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(1,arg0);
			}
        	  
        	  
          });
          JavaRDD<String> flatMap = mapToPair.flatMap(new FlatMapFunction<Tuple2<Integer,String>,String>(){

			@Override
			public Iterable<String> call(Tuple2<Integer, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0._2.split(" "));
			}
        	  
        	  
          });
          
          flatMap.foreach(new VoidFunction<String>(){

			@Override
			public void call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
        	  
          });
          
//          JavaRDD<String> words =textFile.flatMap(new FlatMapFunction<String,String>(){
//
//  			@Override
//  			public Iterable<String> call(String arg0) throws Exception {
//  				// TODO Auto-generated method stub
//  				return Arrays.asList(arg0.split(" "));
//  			}
//  			 
//  			 
//  			 
//  		 });
//  		 
//  		 JavaPairRDD<String, Integer> mapToPair = words.mapToPair(new PairFunction<String, String, Integer>() {
//
//  			@Override
//  			public Tuple2<String, Integer> call(String arg0) throws Exception {
//  				// TODO Auto-generated method stub
//  				int i=1;
//  				return new Tuple2(arg0,1);
//  			}
//  			 
//  			 
//  		});
//  		 
//  		 
//  		 mapToPair.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>(){
//
//			@Override
//			public void call(Tuple2<String, Iterable<Integer>> arg0) throws Exception {
//				// TODO Auto-generated method stub
//				System.out.println(arg0);
//			}
//  			 
//  			 
//  			 
//  		 });
//  		 
//  		 
//  		 
  		 
//  		 JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
//
//  			@Override
//  			public Integer call(Integer arg0, Integer arg1) throws Exception {
//  				// TODO Auto-generated method stub
//  				return arg0+arg1;
//  			}
//  		});
//  		 
//  		 reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
//  			
//  			@Override
//  			public void call(Tuple2<String, Integer> arg0) throws Exception {
//  				// TODO Auto-generated method stub
//  				System.out.println(arg0);
//  			}
//  		});
  		 
  		 
          
          
          
          
          
          
     	 
      }
}
