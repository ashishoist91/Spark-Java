package com.ashish.spark.basics.rdds;


import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCountDemo {
	
	
	
	public static void main(String args[]){
		
		
		SparkConf conf = new SparkConf().setMaster( "local" ).setAppName(
				  "ApacheSparkForJavaDevelopers" );
		JavaSparkContext javaSparkContext = new JavaSparkContext( conf );
		
		//saveAsTextFile()
		JavaRDD<Integer> intRDD = javaSparkContext.parallelize(Arrays.asList(1, 2, 3,4,5),3);
		intRDD.saveAsTextFile("TextFileDir1");
		JavaRDD<String> textRDD= javaSparkContext.textFile("TextFileDir1");
		textRDD.foreach(new VoidFunction<String>() {
		public void call(String x) throws Exception {
		System.out.println("The elements read from TextFileDir are :"+x); }
		});
		
		
	}

}