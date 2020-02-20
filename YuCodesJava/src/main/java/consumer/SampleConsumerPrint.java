package consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class SampleConsumerPrint {

	public static String prettyPrintJsonString(JsonNode jsonNode) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			Object json = mapper.readValue(jsonNode.toString(), Object.class);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
		} catch (Exception e) {
			return "Sorry, pretty print didn't work";
		}
	}

	// Declare a new consumer.
	public static KafkaConsumer consumer;

	public static void main(String[] args) throws IOException {

		InputStream input = SampleConsumerPrint.class.getClassLoader().getResourceAsStream("config.properties");
		Properties props = new Properties();
		props.load(input);

		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat(props.getProperty("date.format"));

		consumer = new KafkaConsumer<String, String>(props);

		// Subscribe to the topic.
		List<String> topics = new ArrayList<>();
		topics.add(props.getProperty("stream.topic"));
		consumer.subscribe(topics);

		// Set the timeout interval for requests for unread messages.
		long pollTimeout = Long.parseLong(props.getProperty("poll.timeout"));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
			if (records.isEmpty())
				System.out.println(
						"Consumer " + "-" + " *==*==*==*==*==* Poll consumed zero messages !! *==*==*==*==*==*");
			else {
				System.out.println("Consumer poll consumed " + records.count() + " messages !!");
				for (ConsumerRecord<String, String> record : records) {
					// If the published message is in String Format then just print message using
					// record.value()
					System.out.printf(" timestamp = %s, offset = %d, key = %s, partition = %s, value = %s%n",
							formatter.format(date), record.offset(), record.key(), record.partition(), record.value());

					// If the published message is in JSON Format then just print message using
					// ObjectMapper
					/*
					 * try { ObjectMapper mapper = new ObjectMapper(); JsonNode msg =
					 * mapper.readTree(record.value()); System.out.printf("%s",
					 * prettyPrintJsonString(msg)); } catch (Exception e) { return; }
					 */
				}
				consumer.commitAsync();

			}
		}

	}
}
