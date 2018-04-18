import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    //val ipFile = "file:/home/gappoc/inputs/hellospark_ip" // Should be some file on your local system
    val ipFile = "hdfs://localhost:9000/user/gappoc/spark_ip/hellospark_ip" // Should be some file on your hdfs system
    val conf = new SparkConf().setAppName("Word Count").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ip = sc.textFile(ipFile)
    /*
     * Below we are using the same word count example tweaks to
     * demonstrate variuos possible RDD functions and their effects
     */
    //val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    
    //val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).groupByKey()
    //val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).foldByKey(10)((a,b)=>a+b)
    val wordCounts = ip.flatMap(line => line.split(" ")).map(word => (word, 1)).countByKey()
    //val op1 = wordCounts.lookup("Vishal")
    wordCounts.foreach(println)
    //op1.foreach (println)
    //wordCounts.collectAsMap().foreach(println)
    //wordCounts.mapValues(a=>a/2).foreach(println)
    /*
     * Lines below are used to demonstrate
     * the facility of of common number RDD functions 
     */
    val doubRDD = sc.parallelize(Seq(1.0,2.0,3.0,4.0,5.0,6.0))
    //doubRDD.foreach(println)
   // println("mean of elements:-"+ doubRDD.mean+" sum of elements:- "+doubRDD.sum+ " variance of elements:- "+doubRDD.variance)
    //println("total stats:-> "+doubRDD.stats)
}
}