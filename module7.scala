import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext


object StockPricesAggregate {
 def main(args: Array[String]) {
   val sparkConf = new SparkConf().setAppName("CustTrans").setMaster("local[2]")
   
   
   val ssc = new SparkContext(sparkConf);

 val sqlContext = new org.apache.spark.sql.SQLContext(ssc);
//   val sqlContext = new HiveContext(ssc);

sqlContext.sql("DROP TABLE IF EXISTS stockPrices")
sqlContext.sql("CREATE TABLE IF NOT EXISTS stockPrices (StockName STRING, UnitsTraded INT, SODPrice DECIMAL(8,2), EODPrice DECIMAL(8,2), StockMarket STRING) row format delimited fields terminated by ','")

sqlContext.sql("LOAD DATA LOCAL INPATH '/home/edureka/testData/StockPrices' INTO table stockPrices");

val spData = sqlContext.sql("SELECT * from stockPrices");

val gainedStocks= sqlContext.sql("SELECT StockMarket,(EODPrice - SODPrice) as gain from stockPrices where StockMarket in ('BSE','NSE') and EODPrice > SODPrice");
val maxGainedStocks = gainedStocks.groupBy("StockMarket").agg(max("gain"));


val looserStocks = sqlContext.sql("SELECT StockMarket,(SODPrice - EODPrice) as loose from stockPrices where StockMarket in ('BSE','NSE') and SODPrice > EODPrice");
val maxlostStocks = looserStocks.groupBy("StockMarket").agg(max("loose"));


val minUnitsTraded = spData.groupBy("stockName").agg(min("unitstraded").alias("min_units"));
val maxUnitsTraded = spData.groupBy("stockName").agg(max("unitstraded").alias("max_units"));

val totalsUnitsTraded = sqlContext.sql("select sum(unitstraded) as total_units from stockPrices");
val totalAmountTraded = sqlContext.sql("select sum(UnitsTraded * EODPrice) as Total_traded from stockPrices");

maxGainedStocks.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_maxGain_result1");
maxlostStocks.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_maxloose_result1");
minUnitsTraded.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_minUNits_result1");
maxUnitsTraded.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_maxUNits_result1");
totalsUnitsTraded.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_totUnits_result1");
totalAmountTraded.rdd.coalesce(1).saveAsTextFile("file:///home/edureka/testData/mod7_totAmnt_result1");
 
  }
}
