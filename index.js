import { match } from "./matching.js";
import { cancel as cancelOrder } from "./cancel.js";
import {
    consumer,
    initKafka,
    publishTradesToKafka,
    publishCancellationToKafka,
} from "./kafkaClient.js";
import {
    loadOrderBookFromRedis,
    saveOrderBookToRedis,
} from "./redisClient.js";

(async () => {
    await initKafka(["matching-engine"]);

    // Consume incoming orders and perform matching
    await consumer.run({
        /**
         * Handle messages from both 'orders' and 'cancel' topics.
         */
        eachMessage: async ({ topic, message }) => {
            try {
                const msg = JSON.parse(message.value.toString());

                if (msg.action === 'order') {
                    const order = msg.orderPayload;
                    if (!order.symbol) {
                        console.error("[MATCH] Message missing symbol", order);
                        return;
                    }
                    const book = await loadOrderBookFromRedis(order.symbol);
                    const { trades, updatedBook } = match(order, book);
                    await saveOrderBookToRedis(order.symbol, updatedBook);
                    await publishTradesToKafka(trades);
                } else if (msg.action === 'cancel') {
                    const cancel = msg.cancelPayload;
                    if (!cancel.orderId || !cancel.symbol) {
                        console.error("[MATCH] Message missing orderId or symbol", cancel);
                        return;
                    }
                    const book = await loadOrderBookFromRedis(cancel.symbol);

                    const { updatedBook, cancelled, removedOrder } = cancelOrder(cancel.orderId, book);
                    await saveOrderBookToRedis(cancel.symbol, updatedBook);

                    // Publish cancellation result regardless of success so downstream services can react.
                    await publishCancellationToKafka({
                        orderId: cancel.orderId,
                        symbol: cancel.symbol,
                        cancelled,
                        removedOrder,
                        timestamp: Date.now(),
                    });
                }
            } catch (err) {
                console.error("[MATCH] Error processing message", err);
            }
        },
    });
})().catch((err) => {
    console.error("[MATCH] Fatal error", err);
});