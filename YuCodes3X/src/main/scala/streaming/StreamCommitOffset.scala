package streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka09._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by Vinayaka Meghraj on 9/28/2017.
 */
object StreamCommitOffset {
	def main(args: Array[String]) = {
			if (args.length < 2) {
				System.err.println("Usage: SparkKafkaConsumerDemo <brokers> <topic consume> <topic produce>.")

				/* Command to execute the program
       /opt/mapr/spark/spark-2.1.0/bin/spark-submit
      --class streaming.SparkKafkaConsumer
      --master yarn
      --deploy-mode client
      --driver-memory 1g
      --executor-memory 2g
      --executor-cores 1
      /home/mapr/Spark2X-1.0-SNAPSHOT.jar localhost:9092 /tmp/kafka1:test1 /tmp/kafka1:test1
				 */
				System.exit(1)
			}
			val groupId = "mapr"
					val offsetReset = "earliest"
					val pollTimeout = "1000"
					val Array(broker, topicConsumer, topicProducer) = args

					val sparkConf = new SparkConf().setAppName("StreamCommitOffset")

					val ssc = new StreamingContext(sparkConf, Seconds(2))
					//ssc.checkpoint("~/tmp")

					val topicsSet = topicConsumer.split(",").toSet

					val kafkaParams = Map[String, String](
							//ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
							ConsumerConfig.GROUP_ID_CONFIG -> groupId,
							ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
							"org.apache.kafka.common.serialization.StringDeserializer",
							ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
							"org.apache.kafka.common.serialization.StringDeserializer",
							ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
							ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
							//ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000",
							//"spark.kafka.poll.time" -> pollTimeout,
							//"streams.zerooffset.record.on.eof" -> "true"

							)

					val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
					val messages = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, consumerStrategy)
					//messages.count().print()
					messages.foreachRDD { rdd =>
					val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
					print(rdd.count())
					messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
			}

			ssc.start()
			ssc.awaitTermination()

			ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
}