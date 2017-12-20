package examples

import java.lang.Thread._

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
 * Created by Vinayaka Meghraj on 6/23/2017.
 */
object WriteParquet {
	def main(args: Array[String]): Unit = {
			val spark = SparkSession.builder().appName("WriteParquet").config("", "").getOrCreate()

					val input = args(0).toString
					val output = args(1).toString
					val df = spark.read.text(input)
					
				//f.write.option("mapreduce.fileoutputcommitter.algorithm.version", "2")

					for (a <- 1 to 10) {
						df.write.mode(SaveMode.Append).parquet(output)
						sleep(2000)
						println("Write count :" + a);
					}
	}
}
