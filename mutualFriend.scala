//@author akshayrawat91

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object mutualFriend {

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
      "\t"+cm

    }

  def main(args: Array[String])  {
    val logFile = "/home/akshay/Documents/bigdata/soc-LiveJournal1Adj.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("mutual friend").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val mf = logData.flatMap(pairs).reduceByKey(fCount)
    mf.repartition(1).saveAsTextFile("/home/akshay/Documents/bigdata/assignment2/out1")
    sc.stop()
  }

}
