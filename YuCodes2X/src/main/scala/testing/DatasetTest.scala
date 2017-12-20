package testing

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Vinayaka Meghraj on 9/13/2017.
  */
object DatasetTest {

  val spark = SparkSession.builder().appName("DatasetTest").config("", "").getOrCreate()

  //org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this);

  case class SimpleTuple(id: Int, desc: String)

  val dataList = List(
    SimpleTuple(5, "abc"),
    SimpleTuple(6, "bcd")
  )

  import spark.implicits._
  val dataset = spark.createDataset(dataList)
  DatasetTest.dataset.show(10)
}





