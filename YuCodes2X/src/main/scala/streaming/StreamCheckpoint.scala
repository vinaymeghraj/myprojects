package streaming

/**
 * Created by Vinayaka Meghraj on 7/25/2017.
 */
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object StreamCheckpoint {

	def main(args: Array[String]) {

		val sc = SparkSession.builder().master("yarn").appName("StreamCheckpoint").getOrCreate().sqlContext.sparkContext
				val checkpointDir = "/tmp/maprtest"
				val defaultStream = args(0).toString()
				val defaultTopic = args(0).toString()

				def factoryFunc(): StreamingContext = {
						val ssc = new StreamingContext(sc, Seconds(20))
								ssc.checkpoint(checkpointDir)
								val topics = defaultStream + ":" + defaultTopic;
						val topicsSet = topics.split(",").toSet
								val kafkaParams = Map[String, String](
										ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "",
										ConsumerConfig.GROUP_ID_CONFIG -> "mapr",
										ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
										"org.apache.kafka.common.serialization.StringDeserializer",
										ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
										"org.apache.kafka.common.serialization.StringDeserializer",
										ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
										ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
										"streams.consumer.default.stream" -> defaultStream,
										"spark.kafka.poll.time" -> "120000")

								val consumerStrategy =ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
								val messages = KafkaUtils.createDirectStream[String, String](
										ssc, LocationStrategies.PreferConsistent, consumerStrategy)

								messages.foreachRDD(rdd=> {
									rdd.foreach(data => {
										println(data.value())
									})
								})

								ssc
				}

				val ssc = StreamingContext.getOrCreate(checkpointDir, factoryFunc _)

						ssc.start()
						ssc.awaitTermination()
	}
}