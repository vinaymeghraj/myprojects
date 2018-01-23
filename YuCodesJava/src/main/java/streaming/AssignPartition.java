package streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by Vinayaka Meghraj on 1/22/2018.
 */
public class AssignPartition {

    private static final Logger LOGGER = LoggerFactory.getLogger(AssignPartition.class);

    /*
    /opt/mapr/spark/spark-2.1.0/bin/spark-submit --class streaming.AssignPartition  --master yarn --deploy-mode client --driver-memory 1g --executor-memory 2g --num-executors 5 --executor-cores 1  /home/mapr/YuCodesJava/target/YuCodesJava-0.0.1-SNAPSHOT.jar /tmp/kafka:test 0 mapr

     */

    //Declare new Consumer
    public static KafkaConsumer consumer;

    public static String groupId = null;

    public static void main(String args[]) throws Exception {

        //args[0] read stream:topic
        String topic = args[0];

        // args[1] get Partition number
        int paritionId = Integer.parseInt(args[1]);

        //args[2] get Group ID
        groupId = args[2];

        // Subscribe to the topic.
        List<String> topics = new ArrayList<>();
        topics.add(topic);

        configureConsumer(args);

        //If we are using assign partitions no need to use subscribe
        //consumer.subscribe(topics);

        TopicPartition topicPartition = new TopicPartition(topic, paritionId);

        //Assignment to partition
        consumer.assign(Arrays.asList(topicPartition));
        LOGGER.warn("Partition assigned ...");
        // Set the timeout interval for requests for unread messages.
        long pollTimeout = 1000;


        boolean stop = false;
        while (!stop) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);
            LOGGER.warn("Iterating the consumer records ...");
            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    // Iterate through returned records, extract the value
                    // of each message, and print the value to standard output.
                    LOGGER.warn(("Earliest offset of the Partition --> " + " Partition ID : " + record.partition() + " Offset : " + record.offset()));
                    break;
                }
            } else {
                stop = true;
            }
        }
        consumer.close();
        LOGGER.warn("All done.");
    }


    public static void configureConsumer(String[] args) {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", "localhost:9092");
        consumer = new KafkaConsumer<String, String>(props);
    }
}