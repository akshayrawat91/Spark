
package akshay.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
 
object mutualFriend {
  
  def pairs(line: String) = {
    val lineSplit = line.split("\t")
    val user = lineSplit(0)
    
    if(lineSplit.length > 1){
      val friends = lineSplit(1).split(",")
      val n = friends.length
      for(i <- 0 to n-1) yield {
        val pair = if(Integer.parseInt(user) < Integer.parseInt(friends(i))){
        (user,friends(i))
        } else {
          (friends(i),user)
        }
        (pair,lineSplit(1))
    
      }
    } 
    
  }
  
  def mutualCount(x: String, y:String) = {
    var v1 = x.split("")
    var v2 = y.split("")
    if(x.split(",").length > 1){
      v1 = x.split(",")
    }
    if(y.split(",").length > 1){
      v2 = y.split(",")
    }
    val v1n = v1.length
    val v2n = v2.length
    var common = 0
    for(i <- 0 until v1n; j <- 0 until v2n) yield {
      if(v1(i) == v2(j)){
        common = common +1        
      }
    }
    val cm = common.toString()
    "\t"+cm
  }
  
  def main(args: Array[String]) {
    
     //Create conf object
       val conf = new SparkConf()
     .setAppName("mutualFriend")
      
     //create spark context object
     val sc = new SparkContext(conf)
     
    //Check whether sufficient params are supplied
     if (args.length < 2) {
     println("Usage: ScalamutualFriend <input> <output>")
     System.exit(1)
     }
     //Read file and create RDD
     val rawData = sc.textFile(args(0))
      
     //convert the lines into words using flatMap operation
     //val mutualfriend = rawData.map(line => (line.split("\t")(0),line.split("\t")(1).length() ))
     
     val mutualfriend = rawData.flatMap(pairs)
     
     
     
     //val mutualfriend = rawData.map(line => (line.split("\t")(0),line.split("\t")(1)) )
     
     
     
      
     //val mutualfriend = words.map(word => (word, 1)).reduceByKey(_ + _)
      
     //Save the result
     mutualfriend.saveAsTextFile(args(1))
     
    //stop the spark context
     sc.stop
 }
}
