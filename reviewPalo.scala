//@author akshayrawat91

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object reviewPalo {

  def manOf[T: Manifest](t: T): Manifest[T] = manifest[T]

  def main(args: Array[String]) {

    val reviewFile = "/home/akshay/Documents/bigdata/review.csv"
    val businessFile = "/home/akshay/Documents/bigdata/business.csv"
    val conf = new SparkConf().setAppName("review Palo Alto").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rData = sc.textFile(reviewFile)
    val bData = sc.textFile(businessFile)

    val review = rData.map(line => (line.split("::")(2),(line.split("::")(1),line.split("::")(3))))
    val business = bData.map(line => if(line.split("::")(1).contains("Palo Alto")) (line.split("::")(0),line.split("::")(1))
      else ("notAvailable","NaN")).distinct().filter(line => !line._1.contains("notAvailable"))

    manOf(review)
    manOf(business)

    val res = business.join(review).map(line => (line._2._2._1, line._2._2._2))

    res.repartition(1).saveAsTextFile("/home/akshay/Documents/bigdata/assignment2/out4")

    sc.stop()


  }
}
