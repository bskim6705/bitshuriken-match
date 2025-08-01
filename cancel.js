// cancel.js

/**
 * Remove an existing order from the in-memory order book by orderId.
 * Keeps price-time priority of remaining orders.
 *
 * @typedef {import('./matching.js').Order} Order
 * @typedef {import('./matching.js').OrderBook} OrderBook
 *
 * @param {string} orderId – unique identifier of the order to cancel
 * @param {OrderBook} book – current order book for the symbol
 * @returns {OrderBook} updatedBook – deep-cloned book without the cancelled order
 */
export function cancel(orderId, book) {
    console.log("recieved cancel", orderId, book);
    let cancelled = false;
    let removedOrder = null;

    // Deep clone & filter bids
    const updatedBook = { bids: {}, asks: {} };

    for (const [price, orders] of Object.entries(book.bids)) {
        const remaining = [];
        for (const o of orders) {
            if (o.orderId === orderId) {
                cancelled = true;
                removedOrder = { ...o };
            } else {
                remaining.push({ ...o });
            }
        }
        if (remaining.length > 0) {
            updatedBook.bids[price] = remaining;
        }
    }

    // Deep clone & filter asks
    for (const [price, orders] of Object.entries(book.asks)) {
        const remaining = [];
        for (const o of orders) {
            if (o.orderId === orderId) {
                cancelled = true;
                removedOrder = { ...o };
            } else {
                remaining.push({ ...o });
            }
        }
        if (remaining.length > 0) {
            updatedBook.asks[price] = remaining;
        }
    }

    console.log("after cancel", updatedBook, cancelled, removedOrder);
    return { updatedBook, cancelled, removedOrder };
} 