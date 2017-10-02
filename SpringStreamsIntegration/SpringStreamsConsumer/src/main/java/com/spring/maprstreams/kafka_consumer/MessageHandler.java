package com.spring.maprstreams.kafka_consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;

@EnableBinding(Sink.class)
@Configuration
public class MessageHandler {
	private static final Logger LOGGER = LoggerFactory.getLogger(MessageHandler.class);


	/*@StreamListener(Sink.INPUT)
	public String handle(final ChatMessage message) {
		final DateTimeFormatter df = DateTimeFormatter.ofLocalizedTime(FormatStyle.MEDIUM).withZone(ZoneId.systemDefault());
		final String time = df.format(Instant.ofEpochMilli(message.getTime()));
		LOGGER.info("[{}]: {}", time, message.getContents());
		System.out.println("Receiving message : " +message.getContents());
		return message.getContents();
	}*/

	@ServiceActivator(inputChannel=Sink.INPUT)
	public void loggerSink(Object payload) {
		System.out.println("Received : " +payload);
	}
}
