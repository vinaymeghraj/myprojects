package structuredStreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Vinayaka Meghraj on 4/23/2018.
  */
object InMemoryWrite {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: InMemoryWrite <windowDuration> <slideDuration>")
      System.exit(1)
    }

    val spark = SparkSession.builder.appName("InMemoryWrite").getOrCreate()

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
    //val query = windowedCounts.writeStream.outputMode("append").format("console").option("truncate", "false").start()

    // Recovering from Failures with Checkpointing

    val query = windowedCounts.writeStream.queryName("activity_counts").format("memory").outputMode("complete").start()

    query.awaitTermination()

    for (i <- 1 to 5) {
      spark.sql("SELECT * FROM activity_counts").show()
      Thread.sleep(5000)
    }
  }
}
