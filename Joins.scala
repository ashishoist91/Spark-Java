import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Joins {
  def main(args: Array[String]) {

	val sc = new SparkContext(new SparkConf().setAppName("Spark Joins").setMaster("local[2]"))

	case class Customer (cust_id: Int, name: String)
	case class Txn (cust_id: Int, store_id: String, amount: Float)


	val custs = sc.textFile("file:/home/gappoc/inputs/custs").map(_.split("\t"))
	val cust_recs = custs.map( r => (r(0).toInt, Customer(r(0).toInt, r(1))))

	val txns = sc.textFile("file:/home/gappoc/inputs/custs_txns").map(_.split("\t"))
	val txns_recs = txns.map( r => (r(0).toInt, Txn(r(0).toInt, r(1), r(2).toFloat)))
	
	println(txns_recs.toDebugString)

	
	/*
	 * The lines below are showing various types of joins
	 * which are as easy as using a keyword!
	 */
	val joind = cust_recs.join(txns_recs)
	
	val leftOuterjoind = cust_recs.leftOuterJoin(txns_recs)
	val cartesianJoined = cust_recs.cartesian(txns_recs)
	val cogrpd = cust_recs.cogroup(txns_recs)

	 //joind.foreach(println)
	//leftOuterjoind.foreach(println)
  //cartesianJoined.foreach(println)
	//cogrpd.foreach(println)
	}
}