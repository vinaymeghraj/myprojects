import org.apache.kafka.clients.consumer.*;

import java.io.IOException;

public class SampleConsumer {
    // Set the stream and topic to read from.
    public static String topic = "/srcDir/srcStream:devtest1";

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) throws IOException {
        configureConsumer(args);

        // Subscribe to the topic.
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer.subscribe(topics);

        // Set the timeout interval for requests for unread messages.
        long pollTimeout = 1000;

        boolean stop = false;
//        int pollTimeout = 1000;
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
        try{
	Thread.sleep(50000);
	}catch(Exception e){
}
        //this.sleep(5000);
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
