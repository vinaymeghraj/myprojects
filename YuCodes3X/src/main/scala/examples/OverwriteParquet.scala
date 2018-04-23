package examples

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object OverwriteParquet {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("OverwriteParquet")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    // Pass the table name to overwrite
    val tableName = args(0).toString()

    val sqlDframe = hiveContext.sql("select * from employee ")

    sqlDframe.write.format("parquet").mode(SaveMode.Overwrite).insertInto(tableName)
  }
}
