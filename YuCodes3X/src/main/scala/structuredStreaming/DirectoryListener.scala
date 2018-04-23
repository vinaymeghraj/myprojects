package structuredStreaming

import org.apache.spark.sql.SparkSession

/**
  * Created by Vinayaka Meghraj on 4/23/2018.
  */
object DirectoryListener {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: DirectoryListener <inputJsonFile> <readFilesFrom> <checkPointPath>")
      System.exit(1)
    }

    val inputJsonFile = args(0).toString
    val readFilesFrom = args(1).toString
    val checkPointPath = args(2).toString

    val spark = SparkSession.builder.appName("DirectoryListener").getOrCreate()

    val static = spark.read.json(inputJsonFile)
    val streaming = spark.readStream.schema(static.schema).option("maxFilesPerTrigger", 10).json(readFilesFrom).groupBy("statusCode").count()
    val query = streaming.writeStream.outputMode("complete").option("checkpointLocation", checkPointPath).queryName("testDirectoryListener").format("memory").start()

    /*
    JSON FIle :
    {"sourceTime":1509818259933,"sourcePicoSeconds":0,"serverTime":1509818312485,"serverPicoSeconds":0,"statusCode":0,"statusCodeStr":"Good","value":11.32066822052002}}
    {"sourceTime":1509818263761,"sourcePicoSeconds":0,"serverTime":1509818312751,"serverPicoSeconds":0,"statusCode":0,"statusCodeStr":"Good","value":245.0}}
    {"sourceTime":1509818263761,"sourcePicoSeconds":0,"serverTime":1509818312751,"serverPicoSeconds":0,"statusCode":0,"statusCodeStr":"Good","value":0.011343427002429962}}
     */
  }
}
