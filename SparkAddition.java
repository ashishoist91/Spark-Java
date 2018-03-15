package com.ashish.spark.basics.rdds;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkAddition {

	public static void main(String[] args) {
		//-master local
		
		SparkConf conf =new SparkConf().setMaster("local").setAppName("jhghjg"); 
		JavaSparkContext javaSparkContext = new JavaSparkContext(conf); 
		
		List<Integer> intList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		
		JavaRDD<Integer> intRDD = javaSparkContext.parallelize( intList , 2);
		
		
		JavaRDD<Integer> result = intRDD.map(x -> x + 1);
		List<Integer> resultist = result.collect();
		for(Integer value : resultist){
			System.out.println("Value : " + value);
		}
		
		

		
		
	}

}
