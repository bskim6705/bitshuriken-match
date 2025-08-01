import Redis from "ioredis";

// Allow overriding host/port via environment variables so the same code works
// both inside Docker and when running locally.
const redis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT) || 6379,
});

export async function loadOrderBookFromRedis(symbol) {
    const key = `orderbook:${symbol}`;
    const orderBook = await redis.get(key);
    return orderBook ? JSON.parse(orderBook) : { bids: {}, asks: {} };
}

export async function saveOrderBookToRedis(symbol, orderBook) {
    const key = `orderbook:${symbol}`;
    await redis.set(key, JSON.stringify(orderBook));

    // Publish an update event so downstream services (e.g. NestJS via SUB) can react.
    // Channel name pattern: "orderbook:update:<SYMBOL>" to allow symbol-specific subscriptions.
    const channel = `orderbook:update:${symbol}`;
    await redis.publish(channel, JSON.stringify({ symbol, orderBook }));
}