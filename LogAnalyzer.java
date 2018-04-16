import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LogAnalyzer {

	public static void main(String[] args) {
		JavaSparkContext javaSparkContext = SparkUtility.getJavaSparkContext(SparkConstant.MASTER_LOCAL, SparkConstant.APP_NAME+"LogAnalyzer");
		JavaRDD<String> logFile = javaSparkContext.textFile("/home/ashish/Desktop/Files/server_log", 1);
		JavaRDD<String> errors = logFile.filter(line -> line.startsWith("ERROR"));
		JavaRDD<String> messages = errors.map(error -> error.split("\t")).map(r -> r[1]);
		messages.cache();
		long total = logFile.count();
		long mysql = messages.filter(message -> message.contains("mysql")).count();
		long php = messages.filter(message -> message.contains("php")).count();
		long rail = messages.filter(message -> message.contains("RailsApp")).count();
		System.out.println("Total msgs: "+total +", MySQL errs: " +mysql +", PHP errs: "+php+ ", Rails: "+rail +" DONE: " + (total - (mysql+php+rail)));

	}

}
