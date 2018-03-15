package com.ashish.spark.basics.rdds;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount { 
	public static void main(String[] args) throws Exception {
		System.out.println(System.getProperty("hadoop.home.dir"));
		String inputPath = args[0];
		String outputPath = args[1];
		//FileUtils.deleteQuietly(new File(outputPath));

		JavaSparkContext sc = new JavaSparkContext("local", "sparkwordcount");

		JavaRDD<String> rdd = sc.textFile(inputPath);

		JavaPairRDD<String, Integer> counts = rdd
				.flatMap(x -> Arrays.asList(x.split(" ")))
				.mapToPair(x -> new Tuple2<String, Integer>((String) x, 1))
				.reduceByKey((x, y) -> x + y);


		
		
		
		counts.saveAsTextFile(outputPath);
		sc.close();
	}
} 