import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

object JoinWithPartitioner {
  def main(args: Array[String]) {

	val sc = new SparkContext(new SparkConf().setAppName("Spark Joins").setMaster("local[2]"))

	case class Customer (cust_id: Int, name: String)
	case class Txn (cust_id: Int, store_id: String, amount: Float)


	val custs = sc.textFile("file:/home/gappoc/inputs/custs").map(_.split("\t"))
	val cust_recs = custs.map( r => (r(0).toInt, Customer(r(0).toInt, r(1))))

	//val txns = sc.textFile("file:/home/gappoc/inputs/custs_txns").partitionBy(new HashPartitioner(2))
	val txns = sc.textFile("file:/home/gappoc/inputs/custs_txns").map(_.split("\t"))
	/*
	 * In the line below, we are partitioning the txns dataset to multiple partitions
	 * This will help when the join will be performed with other dataset
	 * It will result into lesser flow of data in the network
	 */
	val txns_recs = txns.map( r => (r(0).toInt, Txn(r(0).toInt, r(1), r(2).toFloat))).partitionBy(new HashPartitioner(2))

	val joind = cust_recs.join(txns_recs)

	val leftOuterjoind = cust_recs.leftOuterJoin(txns_recs)

	// joind.foreach(println)
	leftOuterjoind.foreach(println)

	}
}