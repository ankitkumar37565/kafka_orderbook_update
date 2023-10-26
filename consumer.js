const { Kafka } = require("kafkajs");
const kafka = new Kafka({
    clientId: "kafkaclient1",
    brokers: ["127.0.0.1:9092"],
});
const consumer = kafka.consumer({
    groupId: "test-group1",
});
const run = async () => {
    await consumer.connect();
    consumer.subscribe({ topic: "orderbook_update", fromBeginning: true });
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                message: message.value.toString(),
            });
        },
    });
};
run().catch(console.error());
