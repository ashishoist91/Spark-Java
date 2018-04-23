import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQLJSON {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(SparkConstant.MASTER_LOCAL, SparkConstant.APP_NAME+"SparkSQL");
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);
		DataFrame employeeDF = sqlContext.read().json("file:///home/hduser/Files/Employee.json");
		
		employeeDF.show();
		employeeDF.printSchema();
		employeeDF.registerTempTable("employee");
		DataFrame empResult = sqlContext.sql("SELECT name, address.city FROM employee WHERE address.state='California'");
		//empResult.collectAsList().forEach(System.out.prin);
		empResult.collectAsList().forEach(System.out::println);
		//employeeDF.saveAsParquetFile("");
		
		employeeDF
	      .write()
	      .format("parquet")
	      .save("file:///home/hduser/Files/Employee.parquet");
		
		//employeeDF.write().save("file:///home/hduser/Files/Employeeparquet");
	}

}
