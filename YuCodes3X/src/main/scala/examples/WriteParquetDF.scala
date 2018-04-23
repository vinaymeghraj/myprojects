package examples

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Vinayaka Meghraj on 10/11/2017.
 */
object WriteParquetDF {
	def main(args: Array[String]): Unit = {
			val sparkConf = new SparkConf().setAppName("WriteParquetDF")
					val sc = new SparkContext(sparkConf)
					val sqlContext = new org.apache.spark.sql.SQLContext(sc)
					val input = args(0).toString
					val output = args(1).toString
					val df = sqlContext.read.text(input)
					df.write.parquet(output)
	}
}
