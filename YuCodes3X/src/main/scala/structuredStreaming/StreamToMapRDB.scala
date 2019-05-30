package structuredStreaming

import com.mapr.db.spark.streaming.MapRDBSourceConfig
import org.apache.spark.sql.SparkSession

/**
  * Created by Vinayaka Meghraj on 5/24/2019.
  */
object StreamToMapRDB {

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: StreamToStream <topicsSrc> <topicsDst> <checkPointLocations>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("StreamToStream").getOrCreate()

    val Array(topicsSrc, topicsDst, checkPointLocation) = args

    //val topicsSrc = "/tmp/kafka1:test"
    //val topicsDst = "/tmp/maprdbtable"
    //val checkPointLocation = "/tmp/checkpoint"


    import org.apache.spark.sql.streaming.Trigger

    //Read from stream
    val stream = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", topicsSrc)
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 10000)
      .load()

    import spark.implicits._

    val query3 = stream.select($"value" as "_id")
      .writeStream.format(MapRDBSourceConfig.Format)
      .option(MapRDBSourceConfig.TablePathOption, topicsDst)
      .option(MapRDBSourceConfig.IdFieldPathOption, "_id")
      .option(MapRDBSourceConfig.CreateTableOption, false)
      .option(MapRDBSourceConfig.BulkModeOption, true)
      .option(MapRDBSourceConfig.SampleSizeOption, 1000)
      .option("checkpointLocation", checkPointLocation)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .queryName("StreamToMapRDB").start()

    query3.awaitTermination()
  }
}
