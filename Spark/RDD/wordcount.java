package Demo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class wordcount {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
         SparkConf conf=new SparkConf().setMaster("local").setAppName("wordcount");
         JavaSparkContext sc=new JavaSparkContext(conf);
         JavaRDD<String> textFile = sc.textFile("E://ScalaTest//a.txt", 2);
		 JavaRDD<String> words =textFile.flatMap(new FlatMapFunction<String,String>(){

			@Override
			public Iterable<String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return Arrays.asList(arg0.split(" "));
			}
			 
			 
			 
		 });
		 
		 JavaPairRDD<String, Integer> mapToPair = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				return new Tuple2(arg0,1);
			}
			 
			 
		});
		 
		 JavaPairRDD<String, Integer> reduceByKey = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer arg0, Integer arg1) throws Exception {
				// TODO Auto-generated method stub
				return arg0+arg1;
			}
		});
		 
		 reduceByKey.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			@Override
			public void call(Tuple2<String, Integer> arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
		});
		 
		 
	}

}
