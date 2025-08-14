import { Kafka } from "kafkajs";

// Futures Kafka client
const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || "matching-engine-futures-service",
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
});

export const producer = kafka.producer();
export const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || "matching-engine-futures-group",
});

/**
 * Connects producer & consumer and subscribes consumer to the given topics.
 * @param {string[]} [subscribeTopics=[]]
 */
export async function initKafka(subscribeTopics = []) {
    await producer.connect();
    await consumer.connect();

    for (const topic of subscribeTopics) {
        await consumer.subscribe({ topic, fromBeginning: false });
    }
}

/**
 * Publish futures trades
 * @param {any[]} trades
 * @param {string} [topic="matching-engine.futures.trades"]
 */
export async function publishTradesToKafka(trades = [], topic = "matching-engine.futures.trades") {
    if (!trades.length) return;

    const messages = trades.map((trade) => ({
        value: JSON.stringify(trade),
    }));

    await producer.send({ topic, messages });
}

/**
 * Publish futures order cancellation
 * @param {object} cancelInfo
 * @param {string} [topic="matching-engine.futures.order.cancelled"]
 */
export async function publishCancellationToKafka(cancelInfo, topic = "matching-engine.futures.order.cancelled") {
    if (!cancelInfo) return;

    await producer.send({
        topic,
        messages: [{ value: JSON.stringify(cancelInfo) }],
    });
}

/**
 * Publish futures balance updates
 * @param {object[]} deltaUserBalances
 * @param {string} [topic="matching-engine.futures.balance.updated"]
 */
export async function publishUpdateBalanceToKafka(deltaUserBalances = [], topic = "matching-engine.futures.balance.updated") {
    if (!deltaUserBalances.length) return;

    const messages = deltaUserBalances.map((deltaUserBalance) => ({
        value: JSON.stringify(deltaUserBalance),
    }));

    await producer.send({
        topic,
        messages,
    });
}


