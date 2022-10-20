package com.example.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMarketConsumer {

	@KafkaListener(topics = "trades", groupId = "market")
	public void handleTradeEvent(String trade) {
		System.out.println("KafkaMarketConsumer has received an event: %s".formatted(trade));
	}
}
