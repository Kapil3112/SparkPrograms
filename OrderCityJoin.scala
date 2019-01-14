#Simple Join program 
#Problem statement

import org.apache.spark.sql.SparkSession

object OrderCityJoin {

def main(args: Array[String]) = {
val spark=SparkSession.builder()
					  .appName("Join Test")
                      .getOrCreate()
val order=spark.read 
			   .option("inferschema","true")
			   .csv("/home/jlcindia/MyData/order.csv")
			   .toDF("id:Int","item","amount","citycode")
val city=spark.read
			  .option("inferschema","true")
		      .csv("/home/jlcindia/MyData/city.csv")
              .toDF("code","name")
val ans=order.join(city,order(citycode)===city(code))   //joining 2 df using condition
	         .drop($"code) 								//dropping one of multiple code column
	         .show
}     
}
