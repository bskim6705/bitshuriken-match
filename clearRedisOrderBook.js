import Redis from "ioredis";
import { Kafka } from "kafkajs";

// 환경 변수로 호스트/포트 오버라이드 가능
const redis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT) || 6379,
});

async function clearOrderBook(symbol) {
    const key = `orderbook:${symbol}`;

    // Redis에서 키 삭제
    await redis.del(key);
    console.log(`Cleared Redis key: ${key}`);

    // 빈 오더북 객체 생성
    const emptyOrderBook = { bids: {}, asks: {} };

    // 빈 오더북을 퍼블리시하여 구독자에게 알림
    const channel = `orderbook:update:${symbol}`;
    await redis.publish(channel, JSON.stringify({ symbol, orderBook: emptyOrderBook }));
    console.log(`Published empty orderBook to channel: ${channel}`);
}

// 모든 Kafka 토픽(내부 토픽 제외) 삭제
async function clearKafkaTopics() {
    const kafka = new Kafka({
        clientId: "clear-utility",
        brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
    });
    const admin = kafka.admin();
    await admin.connect();
    const topics = await admin.listTopics();

    for (const topic of topics) {
        // Kafka 내부 토픽(__consumer_offsets 등)은 삭제하지 않음
        if (topic.startsWith("__")) continue;
        try {
            await admin.deleteTopics({ topics: [topic] });
            console.log(`Deleted Kafka topic: ${topic}`);
        } catch (err) {
            console.error(`Failed to delete topic ${topic}:`, err.message || err);
        }
    }
    await admin.disconnect();
}

async function main() {
    const [, , symbol] = process.argv;
    if (!symbol) {
        console.error("Usage: node clearRedisOrderBook.js <SYMBOL>");
        process.exit(1);
    }

    try {
        await clearOrderBook(symbol);
        await clearKafkaTopics();
        process.exit(0);
    } catch (err) {
        console.error("Error clearing order book:", err);
        process.exit(1);
    }
}

main();