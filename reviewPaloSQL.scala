// @author akshayrawat91

import org.apache.spark.{SparkConf, SparkContext}

case class b2(bId:String, address:String)
case class r2(uId:String, bId2:String, stars:String)

object reviewPaloSQL {

  def main(args: Array[String]) {

    val reviewFile = "/home/akshay/Documents/bigdata/review.csv"
    val businessFile = "/home/akshay/Documents/bigdata/business.csv"
    val conf = new SparkConf().setAppName("review Palo Alto").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rData = sc.textFile(reviewFile)
    val bData = sc.textFile(businessFile)

    val review = rData.map(line => (line.split("::")(2),(line.split("::")(1),line.split("::")(3))))
    val business = bData.map(line => if(line.split("::")(1).contains("Palo Alto")) (line.split("::")(0),line.split("::")(1))
      else ("notAvailable","NaN"))

    // using SparkSQL

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val bs = business.map(line => b2(line._1, line._2)).toDF()
    val rv = review.map(line => r2(line._2._1, line._1, line._2._2.toString)).toDF()

    bs.registerTempTable("bs")
    rv.registerTempTable("rv")

    val q = sqlContext.sql("select uId,stars from rv," +
      "(select distinct(bId) as bId " +
      "from bs where bId <> 'notAvailable') as b" +
      " where rv.bId2 = b.bId")

    //val size = q.count()

    //q.show(size.toInt)

    q.repartition(1).write.csv("/home/akshay/Documents/bigdata/assignment2/out4sql")


  }
}
