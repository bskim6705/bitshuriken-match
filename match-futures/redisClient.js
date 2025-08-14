import Redis from "ioredis";

// Futures Redis client
const redis = new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: Number(process.env.REDIS_PORT) || 6379,
});

export async function loadOrderBookFromRedis(symbol) {
    const key = `futures:orderbook:${symbol}`;
    const orderBook = await redis.get(key);
    return orderBook ? JSON.parse(orderBook) : { bids: {}, asks: {} };
}

export async function saveOrderBookToRedis(symbol, orderBook) {
    const key = `futures:orderbook:${symbol}`;
    await redis.set(key, JSON.stringify(orderBook));

    // publish futures-specific update channel
    const channel = `futures:orderbook:update:${symbol}`;
    await redis.publish(channel, JSON.stringify({ symbol, orderBook }));
}


