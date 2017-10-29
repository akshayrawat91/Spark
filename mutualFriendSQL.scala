
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

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val people = mf.map(line => mf1(line._1._1.trim.toInt,line._1._2.trim.toInt,line._2)).toDF()
    people.registerTempTable("people")
    
    //testing code below
    val q = sqlContext.sql("select * from people")
    val x = q.show()

    sc.stop()

  }
}
