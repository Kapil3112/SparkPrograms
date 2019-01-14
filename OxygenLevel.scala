#Finding max in SparkSql
#Problem statement

import org.apache.spark.sql.SparkSession

object OxygenLevel {
  def main(args: Array[String]) = {
val spark=SparkSession.builder()
		      .appName("Join Test")
              .getOrCreate()

val oxyData=spark.read.format("csv")
                 .option("inferschema","true")
                 .load("/home/jlcindia/MyData/oxylevel.csv")
 		         .toDF("altitude","percentage")
                 .filter($"percentage"<3)
                 .select($"altitude")
         	     .orderBy(desc("percentage"))
		         .head

//Stop the Spark context
    spark.stop
}
