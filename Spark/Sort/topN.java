package Sort;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import scala.Tuple2;

public class topN {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf=null;;
		JavaSparkContext sc=null;
		
		{
			 conf=new SparkConf().setMaster("local").setAppName("PairRDD");
			 sc=new JavaSparkContext(conf);
			
		}
		
//		testTopN(sc);
		testTopN_groupBykey(sc);
	}

	
	//取出前三个分数最高的
	@Test
	public static void testTopN(JavaSparkContext sc){
		
		JavaRDD<String> textFile = sc.textFile("e:\\ScalaTest\\f.txt");

		
		JavaPairRDD<Integer, String> mapToPair1 = textFile.mapToPair(new PairFunction<String,Integer,String>(){

			@Override
			public Tuple2<Integer, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				String[] split = arg0.split(" ");
				String name=split[0];
				Integer score=Integer.valueOf(split[1]);
				return new Tuple2<Integer,String>(score,name);
			}
			
			
			
		});
		
		List<Tuple2<Integer, String>> take = mapToPair1.sortByKey(false).take(3);
		
		System.out.println(take);
		
	}
	
	
	
	
	
	public static void testTopN_groupBykey(JavaSparkContext sc){
		JavaRDD<String> textFile = sc.textFile("e:\\ScalaTest\\f.txt");
		
		 JavaPairRDD<String, Integer> mapToPair1 = textFile.mapToPair(new PairFunction<String,String,Integer>(){

				@Override
				public Tuple2<String, Integer> call(String arg0) throws Exception {
					// TODO Auto-generated method stub
					String[] split = arg0.split(" ");
					String name=split[0];
					Integer score=Integer.valueOf(split[1]);
					return new Tuple2<String,Integer>(name,score);
				}
				
				
				
			});
		JavaPairRDD<String, Iterable<Integer>> groupByKey = mapToPair1.groupByKey();

		JavaPairRDD<String, Iterable<Integer>> mapToPair2 = groupByKey.mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>,String,Iterable<Integer>>(){

			@Override
			public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> arg0) throws Exception {
				// TODO Auto-generated method stub
				
				String name=arg0._1;
				Iterator<Integer> scores = arg0._2.iterator();
				Integer[] score=new Integer[3];
				while(scores.hasNext()){
					Integer value=scores.next();
					
					for(int i=0;i<3;i++){
						if(score[i]==null){
							score[i]=value;
							break;
						}else if(value>score[i]){
							for(int j=2;j>i;j--){
								score[j]=score[j-1];
								
							}
							score[i]=value;
							break;
						}
						
						
					}
									
					
				}
				
				return new Tuple2<String, Iterable<Integer>>(name,Arrays.asList(score));
								
			}
			
			
			
		});
		
		
			
		mapToPair2.sortByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> arg0) throws Exception {
				// TODO Auto-generated method stub
				
				System.out.println("key "+arg0._1);
				Iterator<Integer> scores = arg0._2.iterator();
				while(scores.hasNext()){
					
					Integer value=scores.next();
					
					System.out.print(value+",");
				}
				System.out.println();
			}
			
			
		});
		
		
	
		
	}
	
	
	
	
}
