import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MaxMinSalFinder {
  def getNumericSal(sal : String): String = {
    val newSal = sal.replace("\"","").replace("$","").replace(",","").trim;
    return newSal
  }
  def main(args : Array[String]) {
     if(args.length != 2) 
      println("Usage : spark-submit --class FileContentCount <path to jar>/max-min-salary-finder_2.10-1.0.jar <file to read with full hdfs path> <file to save at hdfs directory>")
     else {
       val sConf = new SparkConf().setAppName("Max and Min Sal Finder App").setMaster("local")
       val sc = new SparkContext(sConf)
       val textFile = sc.textFile(args(0), 1)
       val mappedRdd = textFile.map(line => {val arr = line.split("\t"); val dept = arr(1); val sal = getNumericSal(arr(5)); (dept,sal)})
       val reducedRdd1 = mappedRdd.reduceByKey((a,b) => {if(a>b) a else b})
       val reducedRdd2 = mappedRdd.reduceByKey((a,b) => {if(a>b) b else a})
       val result = reducedRdd1 join reducedRdd2
       result.saveAsTextFile(args(1))
       sc.stop()
     }
  }

}