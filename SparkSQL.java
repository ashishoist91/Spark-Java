import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class SparkSQL {

	private static Integer getNumericSal(String salary) {

		return Integer.parseInt(salary.replace("$", "").replace(",", "").trim());
		
	}
	
	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(SparkConstant.MASTER_LOCAL, SparkConstant.APP_NAME+"SparkSQL");
		@SuppressWarnings("unused")
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(javaSparkContext);
		JavaRDD<Employee> employee = javaSparkContext.textFile("file:///home/hduser/Files/EmpSal").map(line ->{
			String arr[] = line.split("\t");
			return new Employee(arr[0],arr[1],arr[3], getNumericSal(arr[5]));
		});
		DataFrame employeeDF = sqlContext.createDataFrame(employee, Employee.class);
		employeeDF.show();
		employeeDF.printSchema();
		employeeDF.registerTempTable("employee");
		DataFrame empResult = sqlContext.sql("SELECT * FROM employee WHERE salary>40728");
		//empResult.collectAsList().forEach(System.out.prin);
		empResult.collectAsList().forEach(System.out::println);
	}

}
