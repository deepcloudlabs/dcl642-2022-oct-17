package com.example.service;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

@Service
public class RabbitMarketConsumer {

	@RabbitListener(queues = "tradeq")
	public void handleTradeEvent(String trade) {
		System.out.println("RabbitMarketConsumer has received an event: %s".formatted(trade));
	}
}
