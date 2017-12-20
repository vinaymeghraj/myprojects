package examples

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vinayaka Meghraj on 6/15/2017.
  */
object partitionparquet {
  def main(args: Array[String]): Unit = {
    val spark = new SparkConf().setAppName("pi").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val data = Array(1,2,3,4,5,6,7,8,9,1,2,3,4,5,6,7,8,9)
    val dataRDD = sc.parallelize(data);
    println(dataRDD.count()) //output 19

    val textFile = sc.textFile("/user/mockdatain/mockdata.txt") // file size = 4700 MB
    val partitionsCount = textFile.getNumPartitions // partitionsCount: Int = 19 (4700/256MB) - As each block size is 256 MB
    println(textFile.count())
    val repartitionFile = textFile.repartition(30)
    val repartitionCount = repartitionFile.getNumPartitions
    println(repartitionFile.count())



  }
}
