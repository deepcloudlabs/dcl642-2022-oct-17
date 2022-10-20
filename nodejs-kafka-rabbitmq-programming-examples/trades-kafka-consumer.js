const {Kafka} = require('kafkajs')

const {KAFKA_USERNAME: username, KAFKA_PASSWORD: password} = process.env
const sasl = username && password ? {username, password, mechanism: 'plain'} : null
const ssl = !!sasl
const kafka = new Kafka({
    clientId: 'binance-kafka-client',
    brokers: ["localhost:9092"],
    ssl,
    sasl
})

const consumer = kafka.consumer({
    groupId: "binance-kafka"
})


const main = async () => {
    await consumer.connect()

    await consumer.subscribe({
        topic: "trades",
        fromBeginning: true
    })

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log('Received message', {
                topic,
                partition,
                key: message.key,
                value: message.value.toString()
            })
        }
    })
}

main().catch(async error => {
    console.error(error)
    try {
        await consumer.disconnect()
    } catch (e) {
        console.error('Failed to gracefully disconnect consumer', e)
    }
    process.exit(1)
})
