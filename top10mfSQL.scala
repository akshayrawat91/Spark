// @author akshayrawat91

import org.apache.spark.{SparkConf, SparkContext}

case class mf2(user1:Int, user2:Int, fcount:Int)
case class usr2(user:Int, fname:String, lname:String, address:String)

object top10mfSQL {

  def pairs(line: String) = {
    val lineSplit = line.split("\\W+")
    val user = lineSplit(0)

    var friends = ""
    for( i <- 1 until lineSplit.length) yield {
      if(i < lineSplit.length -1)
        friends = friends.concat(lineSplit(i).concat(","))
      else
        friends = friends.concat(lineSplit(i))
    }

    for(i <- 1 until lineSplit.size) yield {
      val pair = if(Integer.parseInt(user) < Integer.parseInt(lineSplit(i))){
        (user,lineSplit(i))
      } else {
        (lineSplit(i),user)
      }
      (pair, friends)
    }

  }

  def fCount(x: String, y:String) = {
    val v1 = x.split("\\W+")
    val v2 = y.split("\\W+")
    val v1n = v1.length
    val v2n = v2.length
    var common = 0
    for(i <- 0 until v1n; j <- 0 until v2n) yield {
      if(Integer.parseInt(v1(i)) == Integer.parseInt(v2(j))){
        common = common +1
      }
    }
    val cm = common.toString()
    cm

  }

  def main(args: Array[String]) {
    val logFile = "/home/akshay/Documents/bigdata/soc-LiveJournal1Adj.txt" // Should be some file on your system
    val userFile = "/home/akshay/Documents/bigdata/userdata.txt"
    val conf = new SparkConf().setAppName("mutual friend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val userData = sc.textFile(userFile)
    val mf = logData.flatMap(pairs).reduceByKey(fCount)
    val usr = userData.map(line => (line.split(",")(0),line.split(",")(1),line.split(",")(2),line.split(",")(3)))

    // using Spark SQL

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val people = mf.map(line => mf2(line._1._1.trim.toInt,line._1._2.trim.toInt,line._2.toInt)).toDF()
    val userinfo = usr.map(line => usr2(line._1.trim.toInt,line._2,line._3,line._4)).toDF()

    people.registerTempTable("people")
    userinfo.registerTempTable("userinfo")

    val top10ppl = sqlContext.sql("select * from people order by fcount desc limit 10")
    //top10ppl.show()
    top10ppl.registerTempTable("ppl")


    val q1 = sqlContext.sql("select ppl.user1 as user1, ppl.user2 as user2, ppl.fcount as fcount, fname, lname, address" +
      " from userinfo, ppl" +
      " where userinfo.user = ppl.user1")
    //q1.show()
    q1.registerTempTable("u1")


    val q2 = sqlContext.sql("select ppl.user1 as user1, ppl.user2 as user2, ppl.fcount as fcount, fname, lname, address" +
      " from userinfo, ppl" +
      " where userinfo.user = ppl.user2")
    //q2.show()
    q2.registerTempTable("u2")

    val q3 = sqlContext.sql("select u1.fcount, u1.fname u1_fname, u1.lname u1_lname, u1.address u1_address, " +
      "u2.fname u2_fname, u2.lname u2_lname, u2.address u2_address from u1,u2" +
      " where u1.user1 = u2.user1 and u1.user2 = u2.user2 ")

    //q3.show()

    q3.repartition(1).write.csv("/home/akshay/Documents/bigdata/assignment2/out2sql")

    sc.stop()

  }
}
