// @author akshayrawat91

import org.apache.spark.{SparkConf, SparkContext}

case class b1(bId:String, address:String, category:String)
case class r1(bId2:String, num:Int)

object top10BusinessSQL {

  def main(args: Array[String]) {

    val reviewFile = "/home/akshay/Documents/bigdata/review.csv"
    val businessFile = "/home/akshay/Documents/bigdata/business.csv"
    val conf = new SparkConf().setAppName("top 10 business").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rData = sc.textFile(reviewFile)
    val bData = sc.textFile(businessFile)

    val review = rData.map(line => line.split("::")(2)).map(line => (line,1))//.reduceByKey(_+_)
    val business = bData.map(line => (line.split("::")(0), line.split("::")(1), line.split("::")(2)))

    // using SparkSQL

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val bs = business.map(line => b1(line._1, line._2, line._3)).toDF()
    val rv = review.map(line => r1(line._1, line._2)).toDF()

    bs.registerTempTable("bs")
    rv.registerTempTable("rv")

    val q = sqlContext.sql("select distinct(bId), address, category,r.num from bs," +
      "(select bId2, count(num) as num from rv " +
      "group by bId2 " +
      "order by num desc" +
      " limit 10) as r where bs.bId = r.bId2 order by r.num desc ")

    //q.show()

    q.repartition(1).write.csv("/home/akshay/Documents/bigdata/assignment2/out3sql")

  }
}
