package structuredStreaming

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Vinayaka Meghraj on 4/23/2018.
  */
object Triggers {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Triggers <windowDuration> <slideDuration>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("Triggers").getOrCreate()

    import spark.implicits._
    val windowDuration = args(0).toString
    val slideDuration = args(1).toString

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).option("includeTimestamp", true).load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line => line._1.split(" ").map(word => (word, line._2))).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(window($"timestamp", windowDuration, slideDuration), $"word").count().orderBy("window")

    import org.apache.spark.sql.streaming.Trigger

    val trigger = windowedCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.ProcessingTime("15 seconds")).start()

    trigger.awaitTermination()

    /*Different options for Trigger

    val trigger = windowedCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.ProcessingTime("15 seconds")).start()
    val trigger = windowedCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.Once()).start()
    val trigger = windowedCounts.writeStream.outputMode("complete").format("console").trigger(Trigger.Continuous("1 second")).start()

    */

  }
}