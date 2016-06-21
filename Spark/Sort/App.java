package Sort;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class App {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkConf conf=new SparkConf().setMaster("local").setAppName("SecordSort");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		
		int number=0;
		Broadcast<Integer> broadcast = sc.broadcast(number);
		
		JavaRDD<String> textFile = sc.textFile("E:\\ScalaTest\\e.txt");
		JavaPairRDD<SecondarySort, String> mapToPair = textFile.mapToPair(new PairFunction<String,SecondarySort,String>(){

			@Override
			public Tuple2<SecondarySort, String> call(String arg0) throws Exception {
				// TODO Auto-generated method stub
                   String[] split = arg0.split(" ");
                   SecondarySort ss=new SecondarySort(Integer.valueOf(split[0]),Integer.valueOf(split[1]));
				   return new Tuple2<SecondarySort,String>(ss,arg0);
				
			}
			
		});
		JavaRDD<String> map = mapToPair.sortByKey().map(new Function<Tuple2<SecondarySort,String>,String>(){

			@Override
			public String call(Tuple2<SecondarySort, String> arg0) throws Exception {
				// TODO Auto-generated method stub
				//过滤掉排序后自定义的KEY，保留排序的结果
				return arg0._2;
			}
			
		});
		map.foreach(new VoidFunction<String>(){

			@Override
			public void call(String arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
			
		});
	}

}
