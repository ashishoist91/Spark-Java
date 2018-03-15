import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.HiveContext;


public class SparkHiveBasic {
	
	
	public static void main(String []args){
		
		
		 JavaSparkContext sc = new JavaSparkContext("local", "SparkSql");

	        HiveContext hiveContext = new HiveContext(sc);
	        hiveContext.sql("select * from hivedb.comedy").show();
		
	}

}
