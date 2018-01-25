package streaming;

/**
 * Created by Vinayaka Meghraj on 1/24/2018.
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class SampleProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleProducer.class);

    // Declare a new producer.
    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        configureProducer(args);

        // Set the stream and topic to publish to.
        String topic = args[0];
        // Set the number of messages to send.
        int numMessages = Integer.valueOf(args[1]);

        for (int i = 0; i < numMessages; i++) {
            // Set content of each message.
            String messageText = "Msg " + i;

           /* Add each message to a record. A ProducerRecord object
              identifies the topic or specific partition to publish
	       a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, messageText);

            // Send the record to the producer client library.
            producer.send(rec);
            LOGGER.warn("Sent message number " + i);
        }
        producer.close();
        LOGGER.warn("All Done");
    }

    /* Set the value for a configuration parameter.
       This configuration parameter specifies which class
       to use to serialize the value of each message.*/
    public static void configureProducer(String[] args) {
        Properties props = new Properties();
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }
}