package Demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.reflect.ClassTag;

public class RDD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		SparkConf conf=new SparkConf().setMaster("local").setAppName("RDDTEST");
		JavaSparkContext sc=new JavaSparkContext(conf);
		
		List list=Arrays.asList(1,2,3,4,5);
		JavaRDD parallelize = sc.parallelize(list);
		JavaRDD filter = parallelize.filter(new Function<Integer, Boolean>() {

			@Override
			public Boolean call(Integer arg0) throws Exception {
				// TODO Auto-generated method stub
				return arg0>4;
			}
			
		});
		List list1=Arrays.asList("error","hadoop");
		JavaRDD parallelize1 = sc.parallelize(list1);
		JavaRDD<String> filter2 = parallelize1.filter(x -> x.equals("hadoop"));
		
		filter2.foreach(new VoidFunction(){

			@Override
			public void call(Object arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
			
			
		});
		
		
		filter.foreach(new VoidFunction(){

			@Override
			public void call(Object arg0) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(arg0);
			}
			
			
		});
		
	}

}
