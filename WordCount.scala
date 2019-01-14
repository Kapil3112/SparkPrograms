#Word Count example

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    //Read some example file to a test RDD
    val test = sc.textFile("C:/Users/kmalviya/Desktop/tempdoc/Data.txt")
   println(args(0))
   val result= test.flatMap { line => //for each line
      line.split("|") //split the line in word by word.
    }
      .map { word => //for each word
        (word, 1) //Return a key/value tuple, with the word as key and 1 as value
      }
      .reduceByKey(_ + _) //Sum all of the value with same key
      .collect
   result.foreach(println)
    //Stop the Spark context
    sc.stop
  }
  
}
