// cancel.js (futures)
import Decimal from 'decimal.js';

class DeltaUserBalance {
    /**
     * @param {number} userId
     * @param {Decimal} unlockBalance
     * @param {Decimal} creditBalance
     */
    constructor(userId, currencyCode, {
        unlockBalance = null,
        creditBalance = null,
        debitBalance = null,
    }
    ) {
        this.userId = userId;
        this.currencyCode = currencyCode;
        this.unlockBalance = unlockBalance;
        this.creditBalance = creditBalance;
        this.debitBalance = debitBalance;
    }
}

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
export function cancel(cp, bk) {
    const cancelPayload = cp;
    const book = bk;
    let cancelled = false;
    let removedOrder = null;
    let deltaUserBalances = [];

    // Deep clone & filter bids
    const updatedBook = { bids: {}, asks: {} };

    for (const [price, orders] of Object.entries(book.bids)) {
        const remaining = [];
        for (const o of orders) {
            if (o.orderId === cancelPayload.orderId) {
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
            if (o.orderId === cancelPayload.orderId) {
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

    if (removedOrder) {
        deltaUserBalances.push(new DeltaUserBalance(
            cancelPayload.userId,
            removedOrder.side === 'SELL' ? cancelPayload.baseCurrencyCode : cancelPayload.quoteCurrencyCode,
            {
                unlockBalance: removedOrder.side === 'SELL' ? new Decimal(removedOrder.qty) : new Decimal(removedOrder.qty).mul(new Decimal(removedOrder.price)),
            }
        ));
    }

    return { updatedBook, cancelled, removedOrder, deltaUserBalances };
}


