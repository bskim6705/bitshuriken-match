import { Kafka } from "kafkajs";

// Basic Kafka client shared by the matching service.
// Connection details are taken from environment variables so that the code
// can run both inside Docker ("kafka:9092") and locally ("localhost:9092").
const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || "matching-engine-service",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

// Re-usable producer and consumer instances.
export const producer = kafka.producer();
export const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "matching-engine-group",
});

/**
 * Connects producer & consumer and subscribes consumer to the given topics.
 * Should be called once before you start consuming messages.
 *
 * @param {string[]} [subscribeTopics=[]] Kafka topics to subscribe to.
 */
export async function initKafka(subscribeTopics = []) {
    await producer.connect();
    await consumer.connect();

    for (const topic of subscribeTopics) {
        await consumer.subscribe({ topic, fromBeginning: false });
    }
}

/**
 * Publishes an array of trade objects to Kafka.
 *
 * @param {any[]} trades        List of trade objects to publish.
 * @param {string} [topic="trades"] Kafka topic name.
 */
export async function publishTradesToKafka(trades = [], topic = "matching-engine.trades") {
    if (!trades.length) return; // Nothing to publish.

    const messages = trades.map((trade) => ({
        value: JSON.stringify(trade),
    }));

    await producer.send({ topic, messages });
}

/**
 * Publishes a single cancellation event to Kafka.
 *
 * @param {object} cancelInfo        JSON-serializable cancellation info.
 * @param {string} [topic="order-cancellations"] Kafka topic name.
 */
export async function publishCancellationToKafka(cancelInfo, topic = "matching-engine.order.cancelled") {
    if (!cancelInfo) return;

    await producer.send({
        topic,
        messages: [{ value: JSON.stringify(cancelInfo) }],
    });
}

/**
 * Publishes an update balance event to Kafka.
 *
 * @param {object} updateInfo        JSON-serializable update balance info.
 * @param {string} [topic="balance-updates"] Kafka topic name.
 */
export async function publishUpdateBalanceToKafka(deltaUserBalances = [], topic = "matching-engine.balance.updated") {
    if (!deltaUserBalances.length) return;

    const messages = deltaUserBalances.map((deltaUserBalance) => ({
        value: JSON.stringify(deltaUserBalance),
    }));

    await producer.send({
        topic,
        messages,
    });
}