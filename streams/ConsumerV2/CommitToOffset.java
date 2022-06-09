import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collections;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;


public class CommitToOffset {
    public static void main(String[] args) {
	
		
        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args[0]);

		// create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        String topic = args[1];
		
		int paritition = Integer.parseInt(args[2]);
		
		// assign and seek are mostly used to replay data or fetch a specific message

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic, paritition);
        
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		long offset = Long.parseLong(args[3]);


        // seek
        consumer.seek(partitionToReadFrom, offset);
		
    	System.out.println("Committing offset for groupID:" + args[0] + " topic:" + args[1] + " paritition:" + args[2] + " offset:" + args[3]);

		
		consumer.commitSync(Collections.singletonMap(partitionToReadFrom, new OffsetAndMetadata(offset + 1)));
	
    }
}
