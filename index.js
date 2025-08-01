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
    // Initialise Kafka (subscribe to the 'orders' topic)
    await initKafka(["orders", "order-cancellations"]);

    // Consume incoming orders and perform matching
    await consumer.run({
        /**
         * Handle messages from both 'orders' and 'cancel' topics.
         */
        eachMessage: async ({ topic, message }) => {
            try {
                const order = JSON.parse(message.value.toString());
                // console.log("received topic, message:", topic, order);

                if (!order.symbol) {
                    console.error("[MATCH] Message missing symbol", order);
                    return;
                }

                const book = await loadOrderBookFromRedis(order.symbol);

                if (topic === "orders") {
                    const { trades, updatedBook } = match(order, book);
                    await saveOrderBookToRedis(order.symbol, updatedBook);
                    await publishTradesToKafka(trades);
                } else if (topic === "order-cancellations") {
                    if (!order.orderId) {
                        console.error("[MATCH] Cancel message missing orderId", order);
                        return;
                    }
                    const { updatedBook, cancelled, removedOrder } = cancelOrder(order.orderId, book);
                    await saveOrderBookToRedis(order.symbol, updatedBook);

                    // Publish cancellation result regardless of success so downstream services can react.
                    await publishCancellationToKafka({
                        orderId: order.orderId,
                        symbol: order.symbol,
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