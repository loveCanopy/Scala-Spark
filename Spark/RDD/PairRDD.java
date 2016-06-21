package Demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class PairRDD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf=null;;
		JavaSparkContext sc=null;
		
		{
			 conf=new SparkConf().setMaster("local").setAppName("PairRDD");
			 sc=new JavaSparkContext(conf);
			
		}
		
		Join(sc);
		groupbykey(sc);
		cogroup(sc);
	}

	
	
	
	public static void Join(JavaSparkContext sc){
	      List<Tuple2<Integer,String>> names = Arrays.asList(new Tuple2(1,"spark"),new Tuple2(2,"scala"),new Tuple2(3,"hadoop"));
		  List<Tuple2<Integer,Integer>> scores = Arrays.asList(new Tuple2(1,100),new Tuple2(2,90),new Tuple2(3,90));
		  JavaPairRDD<Integer, String> p_names = sc.parallelizePairs(names);
		  JavaPairRDD<Integer, Integer> p_scores = sc.parallelizePairs(scores);
		  JavaPairRDD<Integer, Tuple2<String, Integer>> join = p_names.join(p_scores);
		  join.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>(){

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0._1+" "+arg0._2._1+" "+arg0._2._2);
			}
			  
			  
		  });
	}
	
	public static void groupbykey(JavaSparkContext sc){
		List<Tuple2<Integer,String>> asList = Arrays.asList(new Tuple2(100,"spark"),new Tuple2(100,"scala"),new Tuple2(90,"hadoop"),new Tuple2(90,"java"));
		JavaPairRDD<Integer, String> parallelizePairs = sc.parallelizePairs(asList, 1);
		parallelizePairs.groupByKey().foreach(new VoidFunction<Tuple2<Integer,Iterable<String>>>(){

			@Override
			public void call(Tuple2<Integer, Iterable<String>> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
			
		});
	}
	
	
	public static void cogroup(JavaSparkContext sc){
		List<Tuple2<Integer,String>> names = Arrays.asList(new Tuple2(1,"spark"),new Tuple2(2,"scala"),new Tuple2(3,"hadoop"));
		  List<Tuple2<Integer,Integer>> scores = Arrays.asList(new Tuple2(1,100),new Tuple2(2,90),new Tuple2(3,90));
		  JavaPairRDD<Integer, String> p_names = sc.parallelizePairs(names);
		  JavaPairRDD<Integer, Integer> p_scores = sc.parallelizePairs(scores);
		  p_names.cogroup(p_scores).foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>(){

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0._1+" "+arg0._2._1+" "+arg0._2._2);
			}
			  
			  
			  
		  });
	}
	
	
	
}
