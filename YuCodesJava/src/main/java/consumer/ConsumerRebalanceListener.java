package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;


/**
 * @author Vinayaka Meghraj
 *         Command to execute this program
 *         mvn exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath -verbose:class hdfs.CreateDirectory" >> out2.txt 2>&1
 *         java -cp $(hadoop classpath):YuCodesJava/target/YuCodesJava-0.0.1-SNAPSHOT.jar hdfs.CreateDirectory
 */
public class ConsumerRebalanceListener {
    // Set the stream and topic to read from.
    public static String topic = "/s2:petrol";

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) throws IOException, InterruptedException {
        configureConsumer(args);

        // Subscribe to the topic.
        List<String> topics = new ArrayList<>();
        topics.add(args[0]);


        ConsumerRebalanceListener listener = new ConsumerRebalanceListener() {

            /*@Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("Partition Revoked");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("Partition Assigned");
            }*/
        };

        consumer.subscribe(topics, (org.apache.kafka.clients.consumer.ConsumerRebalanceListener) listener);

        for (Object obj : consumer.assignment()) {
            TopicPartition tp = (TopicPartition) obj;

            System.out.println("Assigned partition: " + tp.topic() + ":" + tp.partition() );
        }

        // Set the timeout interval for requests for unread messages.
        long pollTimeout = 1000;

        boolean stop = false;
        while (!stop) {
            // Request unread messages from the topic.
            ConsumerRecords<String, String> consumerRecords = consumer.poll(pollTimeout);

            Iterator<ConsumerRecord<String, String>> iterator = consumerRecords.iterator();
            if (iterator.hasNext()) {
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();
                    // Iterate through returned records, extract the value
                    // of each message, and print the value to standard output.
                    System.out.println((" Consumed Record: " + record.toString()));
                }
            } else {
                stop = true;
            }
        }
        consumer.close();
        System.out.println("All done.");
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to deserialize the value of each message.*/
    public static void configureConsumer(String[] args) {
        Properties props = new Properties();
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");


        consumer = new KafkaConsumer<String, String>(props);
    }

}


