const Websocket = require('ws');
const binanceWssUrl = "wss://stream.binance.com:9443/ws/btcusdt@trade";
const ws = new Websocket(binanceWssUrl);

const {Kafka,Partitioners } = require('kafkajs')

const {KAFKA_USERNAME: username, KAFKA_PASSWORD: password} = process.env
const sasl = username && password ? {username, password, mechanism: 'plain'} : null
const ssl = !!sasl

ws.on('open', async () => {
    const kafka = new Kafka({
        clientId: 'binance-kafka-client',
        brokers: ["localhost:9092"],
        ssl,
        sasl
    })
    const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

    await producer.connect();
    ws.on('message', (frame) => {
        let trade = JSON.parse(frame);
        let volume = Number(trade.p) * Number(trade.q);
        trade.volume = volume;
        producer.send({
            topic: "trades",
            messages: [{
                key: '',
                value: JSON.stringify(trade)
            }]
        })
    });
})

