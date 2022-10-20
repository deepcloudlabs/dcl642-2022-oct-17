let amqp = require('amqplib/callback_api');

amqp.connect('amqp://guest:guest@localhost:7100', (err, connection) => {
    if (err) {
        throw err;
    }
    connection.createChannel((err, channel) => {
        if (err) {
            throw err;
        }
        channel.consume('tradeq', msg => {
            if (msg.content) {
                console.log(msg.content.toString());
            }
        }, {noAck: true, exclusive: false});
    });
});