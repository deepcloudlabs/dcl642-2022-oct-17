package com.example.service;

import javax.annotation.PostConstruct;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;

@Service
public class BinanceMarketService implements WebSocketHandler {
	private final WebSocketClient webSocketClient;
	private final RabbitTemplate rabbitTemplate;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public BinanceMarketService(WebSocketClient webSocketClient, RabbitTemplate rabbitTemplate,
			KafkaTemplate<String, String> kafkaTemplate) {
		this.webSocketClient = webSocketClient;
		this.rabbitTemplate = rabbitTemplate;
		this.kafkaTemplate = kafkaTemplate;
	}

	@PostConstruct
	public void connectToBinance() {
		webSocketClient.doHandshake(this, "wss://stream.binance.com:9443/ws/btcusdt@trade");
	}

	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		System.out.println("Connected to the binance server");
	}

	@Override
	public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
		String tradeEvent = message.getPayload().toString();
		rabbitTemplate.convertAndSend("tradex", null, tradeEvent);
		kafkaTemplate.send("trades", tradeEvent);
	}

	@Override
	public void handleTransportError(WebSocketSession session, Throwable e) throws Exception {
		System.out.println("Error has occured: %s".formatted(e.getMessage()));
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus closeStatus) throws Exception {
		System.out.println("Disconnected from the binance server");
	}

	@Override
	public boolean supportsPartialMessages() {
		return false;
	}
}
