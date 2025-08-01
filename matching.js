// matching.js

/**
 * Pure in-memory order matching implementation.
 * Uses price-time priority: price levels sorted, and FIFO per level.
 * Supports LIMIT and MARKET order types.
 *
 * @typedef {Object} Order
 * @property {string} orderId
 * @property {'BUY'|'SELL'} side
 * @property {'LIMIT'|'MARKET'} type
 * @property {number} price    // Only for LIMIT orders
 * @property {number} qty
 *
 * @typedef {Object<string, Order[]>} BookSide
 * @typedef {Object} OrderBook
 * @property {BookSide} bids  // price -> list of buy orders
 * @property {BookSide} asks  // price -> list of sell orders
 */

/**
 * Trade represents a matched transaction between a maker and taker.
 * @typedef {Object} MakerTakerInfo
 * @property {string} orderId
 * @property {'BUY'|'SELL'} side
 *
 * @typedef {Object} Trade
 * @property {string} symbol
 * @property {MakerTakerInfo} maker
 * @property {MakerTakerInfo} taker
 * @property {number} price
 * @property {number} qty
 * @property {number} timestamp
 */

class Trade {
    /**
     * @param {{ symbol: string, maker: MakerTakerInfo, taker: MakerTakerInfo, price: number, qty: number, timestamp: number }} args
     */
    constructor({ symbol, maker, taker, price, qty, timestamp }) {
        if (!maker || !taker || price == null || qty == null || timestamp == null) {
            throw new Error('Invalid trade parameters');
        }
        this.symbol = symbol;
        this.maker = maker;
        this.taker = taker;
        this.price = price;
        this.qty = qty;
        this.timestamp = timestamp;
    }
}

// TPS counter initialized once per process
let tradeCount = 0;
setInterval(() => {
    console.log(`TPS: ${tradeCount}`);
    tradeCount = 0;
}, 1000);

/**
 * Match an incoming order against the in-memory order book.
 * @param {Order} order
 * @param {OrderBook} book
 * @returns {{ trades: Trade[], updatedBook: OrderBook }}
 */
export function match(order, book) {

    // Deep clone book
    const updatedBook = { bids: {}, asks: {} };
    for (const [p, orders] of Object.entries(book.bids)) {
        updatedBook.bids[p] = orders.map(o => ({ ...o }));
    }
    for (const [p, orders] of Object.entries(book.asks)) {
        updatedBook.asks[p] = orders.map(o => ({ ...o }));
    }

    const trades = [];
    let remaining = order.qty;
    const isBuy = order.side === 'BUY';
    const ownSide = isBuy ? 'bids' : 'asks';
    const oppositeSide = isBuy ? 'asks' : 'bids';

    // Price levels sorted: BUY matches lowest asks first; SELL matches highest bids first
    const levels = Object.keys(updatedBook[oppositeSide])
        .map(Number)
        .sort((a, b) => isBuy ? a - b : b - a);

    for (const priceLevel of levels) {
        if (remaining <= 0) break;
        // For LIMIT orders, enforce price constraint
        if (order.type === 'LIMIT') {
            if ((isBuy && order.price < priceLevel) || (!isBuy && order.price > priceLevel)) {
                break;
            }
        }
        const queue = updatedBook[oppositeSide][priceLevel.toString()];
        while (queue.length > 0 && remaining > 0) {
            const maker = queue.shift();
            const matchQty = Math.min(remaining, maker.qty);

            const trade = new Trade({
                symbol: order.symbol,
                maker: {
                    orderId: maker.orderId,
                    side: maker.side,
                    userId: maker.userId
                },
                taker: {
                    orderId: order.orderId,
                    side: order.side,
                    userId: order.userId
                },
                price: priceLevel,
                qty: matchQty,
                timestamp: Date.now(),
            });
            trades.push(trade);

            remaining -= matchQty;
            maker.qty -= matchQty;
            if (maker.qty > 0) {
                queue.unshift(maker);
                break;
            }
        }
        if (queue.length === 0) {
            delete updatedBook[oppositeSide][priceLevel.toString()];
        }
    }

    // For LIMIT orders, add leftover to own side; MARKET leftovers are discarded
    if (order.type === 'LIMIT' && remaining > 0) {
        const sideBook = updatedBook[ownSide];
        const key = order.price.toString();
        if (!sideBook[key]) sideBook[key] = [];
        sideBook[key].push({
            orderId: order.orderId,
            side: order.side,
            type: order.type,
            price: order.price,
            qty: remaining,
            userId: order.userId,
        });
    }

    tradeCount += trades.length;
    return { trades, updatedBook };
}
