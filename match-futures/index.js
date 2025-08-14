import { match } from "./matching.js";
import { cancel as cancelOrder } from "./cancel.js";
import {
    consumer,
    initKafka,
    publishTradesToKafka,
    publishCancellationToKafka,
    publishUpdateBalanceToKafka,
} from "./kafkaClient.js";
import {
    loadOrderBookFromRedis,
    saveOrderBookToRedis,
} from "./redisClient.js";

/**
 * Futures matching engine entrypoint.
 * Mirrors the structure of spot implementation with futures-specific namespaces
 * (Kafka topics and Redis keys are namespaced to avoid collisions).
 */

(async () => {
    await initKafka(["matching-engine.futures"]);

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            try {
                const msg = JSON.parse(message.value.toString());

                if (msg.action === 'order') {
                    const order = msg.orderPayload;
                    if (!order.symbol) {
                        console.error("[FUTURES MATCH] Message missing symbol", order);
                        return;
                    }
                    const book = await loadOrderBookFromRedis(order.symbol);
                    const { trades, updatedBook, deltaUserBalances } = match(order, book);
                    await Promise.all([
                        saveOrderBookToRedis(order.symbol, updatedBook),
                        publishTradesToKafka(trades),
                        publishUpdateBalanceToKafka(deltaUserBalances),
                    ]);
                } else if (msg.action === 'cancel') {
                    const cancel = msg.cancelPayload;
                    if (!cancel.orderId || !cancel.symbol) {
                        console.error("[FUTURES MATCH] Message missing orderId or symbol", cancel);
                        return;
                    }
                    const book = await loadOrderBookFromRedis(cancel.symbol);
                    const { updatedBook, cancelled, removedOrder, deltaUserBalances } = cancelOrder(cancel, book);
                    await Promise.all([
                        saveOrderBookToRedis(cancel.symbol, updatedBook),
                        publishCancellationToKafka({
                            orderId: cancel.orderId,
                            symbol: cancel.symbol,
                            cancelled,
                            removedOrder,
                            timestamp: Date.now(),
                        }),
                        publishUpdateBalanceToKafka(deltaUserBalances),
                    ]);
                }
            } catch (err) {
                console.error("[FUTURES MATCH] Error processing message", err);
            }
        },
    });
})().catch((err) => {
    console.error("[FUTURES MATCH] Fatal error", err);
});


