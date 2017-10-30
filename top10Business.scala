//@author akshayrawat91

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object top10Business {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    val reviewFile = "/home/akshay/Documents/bigdata/review.csv"
    val businessFile = "/home/akshay/Documents/bigdata/business.csv"
    val conf = new SparkConf().setAppName("top 10 business").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rData = sc.textFile(reviewFile)
    val bData = sc.textFile(businessFile)

    val review = rData.map(line => line.split("::")(2)).map(line => (line,1)).reduceByKey(_+_).sortBy(_._2,false).take(10).map(line => (line._1,line._2))
    val business = bData.map(line => (line.split("::")(0), line.split("::")(1)+"\t"+line.split("::")(2)))

    val rv = sc.parallelize(review)

    manOf(rv)
    manOf(business)

    val res = business.join(rv).distinct().sortBy(_._2._2,false)

    res.repartition(1).saveAsTextFile("/home/akshay/Documents/bigdata/assignment2/out3")

    sc.stop()


  }
}
