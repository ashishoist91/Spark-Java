import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MinMaxSalaryFinder {
	private static Integer getNumericSal(String salary) {

		return Integer.parseInt(salary.replace("\"", "").replace("$", "").replace(",", "").trim());
		
	}
	
	
	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(SparkConstant.MASTER_LOCAL, SparkConstant.APP_NAME+"MinMaxSalaryFinder");
		JavaRDD<String> textFile = javaSparkContext.textFile("/home/ashish/Desktop/Files/EmpSal", 1);

		JavaPairRDD<String, Integer> mappedRdd = textFile.mapToPair(line -> {
												String arr[] = line.split("\t");
												System.out.println("String : " + arr[0] + " -- " + arr[1] + " -- " + arr[5]);
												String dept = arr[1];
												Integer sal = getNumericSal(arr[5]);
												return new Tuple2<String, Integer>(dept,sal);
												});
		JavaPairRDD<String, Integer> reducedRdd1 = mappedRdd.reduceByKey((a,b) -> {
		if(a>b) 
			return a;
		else 
			return b;
		});
		
		JavaPairRDD<String, Integer> reducedRdd2 = mappedRdd.reduceByKey((a,b) -> {
			if(a>b) 
				return b;
			else 
				return a;
			});
		JavaPairRDD<String, Tuple2<Integer, Integer>> result = reducedRdd1.join(reducedRdd2);
		result.saveAsTextFile("/home/ashish/Desktop/Files/MinMaxSalaryFinder");
	
	}

	


}

