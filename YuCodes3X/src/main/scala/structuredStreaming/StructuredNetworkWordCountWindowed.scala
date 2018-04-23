package structuredStreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode


/**
  * Created by Vinayaka Meghraj on 4/23/2018.
  *
  * Usage: StructuredNetworkWordCountWindowed <windowDuration> <slideDuration>
  * <windowDuration> and <slideDuration> describe the TCP server that Spark Streaming would connect to receive data.
  *
  * To run this on your local machine, you need to first run a Netcat server
  *    `$ nc -lk 9999`
  * and then run the example

  */
object StructuredNetworkWordCountWindowed {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StructuredNetworkWordCountWindowed <windowDuration> <slideDuration>")
      System.exit(1)
    }
    val spark = SparkSession.builder.appName("StructuredNetworkWordCountWindowed").getOrCreate()

    import spark.implicits._
    val windowDuration = args(0).toString
    val slideDuration = args(1).toString

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option("includeTimestamp", true).load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(window($"timestamp", windowDuration, slideDuration), $"word").count().orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream.outputMode(OutputMode.Complete()).format("console").option("truncate", "false").start()

    // Recovering from Failures with Checkpointing

    //TODO : val query = windowedCounts.writeStream.outputMode("complete").format("memory").option("checkpointLocation", "file:///tmp/").start()

    query.awaitTermination()

  }
}
