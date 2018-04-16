import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object LogAnalyzer {
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("Server Log Analyzer").setMaster("local[2]"))
		val logFile = "/home/edureka/server_log"
		val lines = sc.textFile(logFile)
		val errors = lines.filter(_.startsWith("ERROR"))
		val messages = errors.map(_.split("\t")).map(r => r(1))
		messages.cache
		val tot = lines.count
		val msql = messages.filter(_.contains("mysql")).count
		val php = messages.filter(_.contains("php")).count
		val rail = messages.filter(_.contains("RailsApp")).count
		/*
			Now this logic right now is printing the statistics on console,
			you could save the results in a file, or DB or even pass message
			to other system here
		*/
		println("Total msgs: %s, MySQL errs: %s, PHP errs: %s, Rails: %s, DONE: %s".format(tot, msql, php, rail, (tot - (msql+php+rail))))
	}
}
