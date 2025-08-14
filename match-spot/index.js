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
 * @typedef {Object} OrderPayload
 * @property {string} orderId
 * @property {'BUY'|'SELL'} side
 * @property {'LIMIT'|'MARKET'} type
 * @property {string} price    // Only for LIMIT orders
 * @property {string} qty
 * @property {string|number} [userId]
 * @property {string} baseCurrencyCode
 * @property {string} quoteCurrencyCode
 *
 * @typedef {Object<string, Order[]>} BookSide
 * @typedef {Object} OrderBook
 * @property {BookSide} bids  // price(string) -> list of buy orders
 * @property {BookSide} asks  // price(string) -> list of sell orders
 */

/** 
 * @typedef {Object} CancelPayload
 */

/**
 * @typedef {Object} DeltaUserBalance
 * @property {number} userId
 * @property {string} currencyCode
 * @property {Decimal} unlockBalance
 * @property {Decimal} creditBalance
 * @property {Decimal} debitBalance
 */



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
                    const { trades, updatedBook, deltaUserBalances } = match(order, book);
                    await Promise.all([
                        saveOrderBookToRedis(order.symbol, updatedBook),
                        publishTradesToKafka(trades),
                        publishUpdateBalanceToKafka(deltaUserBalances),
                    ]);
                } else if (msg.action === 'cancel') {
                    const cancel = msg.cancelPayload;
                    if (!cancel.orderId || !cancel.symbol) {
                        console.error("[MATCH] Message missing orderId or symbol", cancel);
                        return;
                    }
                    const book = await loadOrderBookFromRedis(cancel.symbol);

                    const { updatedBook, cancelled, removedOrder, deltaUserBalances } = cancelOrder(cancel, book);
                    // Persist updated book and publish events concurrently
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
                console.error("[MATCH] Error processing message", err);
            }
        },
    });
})().catch((err) => {
    console.error("[MATCH] Fatal error", err);
});