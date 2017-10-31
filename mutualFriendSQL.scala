// @author akshayrawat91

import org.apache.spark.{SparkConf, SparkContext}

case class mf1(user1:Int, user2:Int, friends:String)

object mutualFriendSQL {

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

  def main(args: Array[String]) {
    val logFile = "/home/akshay/Documents/bigdata/soc-LiveJournal1Adj.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("mutual friend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val mf = logData.flatMap(pairs)

    // using Spark SQL

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val people = mf.map(line => mf1(line._1._1.trim.toInt,line._1._2.trim.toInt,line._2))

    val table1 = people.toDF()
    val table2 = people.toDF()

    table1.registerTempTable("table1")
    table1.registerTempTable("table2")

    val table3 = sqlContext.sql("select table1.user1 as user1, table1.user2 as user2, table1.friends as Afriends, " +
      "table2.friends as Bfriends" +
      " from table1, table2 " +
      "where table1.user1 = table2.user1 and table1.user2 = table2.user2 and table1.friends <> table2.friends")
    table3.registerTempTable("table3")

    val common = (x: String, y:String) => {
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

    sqlContext.udf.register("common", common)

    val table4 = sqlContext.sql("select distinct(user1, user2) as userA_B, common(Afriends, Bfriends) as count " +
      "from table3 order by count desc limit 10")

    //table4.repartition(1).write.csv("/home/akshay/Documents/bigdata/assignment2/out1sql")

    table4.show()

    sc.stop()

  }
}
