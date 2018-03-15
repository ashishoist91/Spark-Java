import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class SparkSql {

	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext("local", "SparkSql");
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
		DataFrame df = sqlContext.read().json("/home/cloudera/Desktop/student.json");
		
		
		df.show();
		df.select("scores").show();
		
		df.printSchema();
		
		
		
		
		

	}

}
