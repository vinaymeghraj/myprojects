package structuredStreaming


import org.apache.spark.sql.SparkSession




/**
  * Created by Vinayaka Meghraj on 6/13/2018.
  */
object StreamToStream {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: StreamToStream <topicsSrc> <topicsDst> <checkPointLocations>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("StreamToStream").getOrCreate()

    val Array(topicsSrc, topicsDst, checkPointLocation) = args

    //val topicsSrc = "/tmp/kafka:test"
    //val topicsDst = "/tmp/kafka1:test"
    //val checkPointLocation = "/tmp/checkpoint"

    //Read from stream
    val stream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", topicsSrc)
      .option("startingOffsets", "earliest")
      .load()


    import org.apache.spark.sql.streaming.Trigger

    //Write toStream
    val query1 = stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("topic", topicsDst)
      .option("group.id", "mapr")
      .option("checkpointLocation", checkPointLocation)
      .outputMode("append").trigger(Trigger.ProcessingTime("20 seconds"))
      .start()

    query1.awaitTermination()
  }
}
