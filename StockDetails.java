import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;

public class StockDetails {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(SparkConstant.MASTER_LOCAL, SparkConstant.APP_NAME+"SparkSQL");
		HiveContext sqlContext = new HiveContext(javaSparkContext);
		//sqlContext.sql("show databases").collectAsList().forEach(System.out::println); 
		sqlContext.sql("DROP TABLE IF EXISTS stockPrices");
		
		sqlContext.sql("CREATE TABLE IF NOT EXISTS stockPrices (StockName STRING, UnitsTraded INT, SODPrice DECIMAL(8,2), EODPrice DECIMAL(8,2), StockMarket STRING) row format delimited fields terminated by ','");
		
		// Load CSV File 
		sqlContext.sql("LOAD DATA LOCAL INPATH '/home/hduser/Files/StockPrices.csv' INTO table stockPrices");
		
		
		// Select All Data
		DataFrame result = sqlContext.sql("SELECT StockName,UnitsTraded,SODPrice,EODPrice,StockMarket from stockPrices");
		//Print All Data 
		System.out.println("All Stock....");
		result.collectAsList().forEach(System.out::println);
		
		
		DataFrame gainedStocks =  sqlContext.sql("SELECT StockMarket,(EODPrice - SODPrice) as gain from stockPrices where StockMarket in ('BSE','NSE') and EODPrice > SODPrice");
		DataFrame maxGainedStocks = gainedStocks.groupBy("StockMarket").agg(functions.max("gain"));
		System.out.println("Highest Gain In Stock");
		maxGainedStocks.collectAsList().forEach(System.out::println);
		
		DataFrame looserStocks = sqlContext.sql("SELECT StockMarket,(SODPrice - EODPrice) as loose from stockPrices where StockMarket in ('BSE','NSE') and SODPrice > EODPrice");
		DataFrame maxlostStocks = looserStocks.groupBy("StockMarket").agg(functions.max("loose"));
		System.out.println("Biggest Loser In Stock");
		maxlostStocks.collectAsList().forEach(System.out::println);
		
		DataFrame minUnitsTraded = result.groupBy("stockName").agg(functions.min("unitstraded").alias("min_units"));
		System.out.println("Max Unit Sold");
		minUnitsTraded.collectAsList().forEach(System.out::println);
		
		DataFrame maxUnitsTraded = result.groupBy("stockName").agg(functions.max("unitstraded").alias("max_units"));
		System.out.println("Min Unit Sold");
		maxUnitsTraded.collectAsList().forEach(System.out::println);
		
		DataFrame totalsUnitsTraded = sqlContext.sql("select sum(unitstraded) as total_units from stockPrices");
		System.out.println("Total Sold Unit");
		totalsUnitsTraded.collectAsList().forEach(System.out::println);
		
		DataFrame totalAmountTraded = sqlContext.sql("select sum(UnitsTraded * EODPrice) as Total_traded from stockPrices");
		System.out.println("Total Tread Amount");
		totalAmountTraded.collectAsList().forEach(System.out::println);
		
		
	}

}
