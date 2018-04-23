package sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vinayaka Meghraj on 6/23/2017.
  */
object context {
  val spark = new SparkConf().setAppName("sqlContext").setMaster("yarn")
  val sc = new SparkContext(spark)
  val sqlContext = new SQLContext(sc)
  val rdd = sqlContext.read.text("")


}
