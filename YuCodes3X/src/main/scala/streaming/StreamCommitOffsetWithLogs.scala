package streaming


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory


/**
  * Created by Vinayaka Meghraj on 1/16/2018.
  */
object StreamCommitOffsetWithLogs {

  object LogHolder {
    @transient lazy val LOG = LoggerFactory.getLogger("test")
  }

  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: SimpleTestDriver <broker bootstrap servers> <topic> <groupId> <offsetReset>")
      System.exit(1)
    }
    /*
    /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class streaming.StreamCommitOffsetWithLogs --master yarn --deploy-mode client --driver-memory 1g --executor-memory 2g --num-executors 5 --executor-cores 1  /home/mapr/YuCodes2X/target/YuCodes2X-0.0.1-SNAPSHOT.jar  localhost:9092 /tmp/kafka:test mapr5 earliest
     */

    val Array(topic, groupId, offsetReset) = args
    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List(topic)

    val kafkaParams = Map(
      //"bootstrap.servers" -> brokers,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> groupId,
      "auto.offset.reset" -> offsetReset,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkConf = new SparkConf().setAppName("StreamCommitOffsetWithLogs")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))


    val messages = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    LogHolder.LOG.warn("Total Message Count ## : " + messages.count().toString)

    // TEST 1
    messages.foreachRDD { rdd =>
      // Get the offset ranges in the RDD and log
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      for (o <- offsetRanges) {
        LogHolder.LOG.warn(s" RDD ##### : ${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach { record =>
          LogHolder.LOG.warn(s" Partition ##### : ${record.key()} ${record.value()} ${record.partition()} ${record.offset()} ")
        }
      }
      //messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    // TEST 2
    /*messages.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        partitionOfRecords.foreach { record =>
          LogHolder.LOG.info(s"${record.key()} ${record.value()} ${record.partition()} ${record.offset()} ")
        }
      }
    }*/

    // TEST 3
    /*messages.transform { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (t <- offsetRanges) {
        LogHolder.LOG.warn(f"Topic: ${t.topic}, Partition: ${t.partition}, fromOffset: ${t.fromOffset}%d, untilOffset: ${t.untilOffset}%d ")
      }
      rdd
    }.foreachRDD {
      rdd =>
        // read in
        rdd.foreach {
          t => LogHolder.LOG.warn(f"Key: ${t.key()}, Value: ${t.value()}, Partition: ${t.partition()}%d, Offset: ${t.offset()}%d, TS: ${t.timestamp()}")
        }
    }*/


    streamingContext.start
    streamingContext.awaitTermination()
    streamingContext.stop(stopSparkContext = true, stopGracefully = true)

  }
}