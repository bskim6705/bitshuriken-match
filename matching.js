// matching.js
import Decimal from 'decimal.js';

/**
 * Pure in-memory order matching implementation.
 * Uses price-time priority: price levels sorted, and FIFO per level.
 * Supports LIMIT and MARKET order types.
 *
 * @typedef {Object} Order
 * @property {string} orderId
 * @property {'BUY'|'SELL'} side
 * @property {'LIMIT'|'MARKET'} type
 * @property {string} price    // Only for LIMIT orders
 * @property {string} qty
 * @property {string|number} [userId]
 *
 * @typedef {Object<string, Order[]>} BookSide
 * @typedef {Object} OrderBook
 * @property {BookSide} bids  // price(string) -> list of buy orders
 * @property {BookSide} asks  // price(string) -> list of sell orders
 */

/**
 * Trade represents a matched transaction between a maker and a taker.
 * filled: this trade leaves the order fully completed (post-trade state).
 *
 * @typedef {Object} MakerTakerInfo
 * @property {string} orderId
 * @property {'BUY'|'SELL'} side
 * @property {string|number} [userId]
 * @property {boolean} [filled]
 *
 * @typedef {Object} TradeLike
 * @property {string} symbol
 * @property {MakerTakerInfo} maker
 * @property {MakerTakerInfo} taker
 * @property {string} price
 * @property {string} qty
 * @property {number} timestamp
 */

class Trade {
    /**
     * @param {{ symbol: string, maker: MakerTakerInfo, taker: MakerTakerInfo, price: string, qty: string, timestamp: number }} args
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
    // Deep clone book (preserve string price keys)
    const updatedBook = { bids: {}, asks: {} };
    for (const [p, orders] of Object.entries(book.bids)) {
        updatedBook.bids[p] = orders.map(o => ({ ...o }));
    }
    for (const [p, orders] of Object.entries(book.asks)) {
        updatedBook.asks[p] = orders.map(o => ({ ...o }));
    }

    const trades = [];
    const orderQtyDecimal = new Decimal(order.qty);
    let remaining = orderQtyDecimal;
    const isBuy = order.side === 'BUY';
    const ownSide = isBuy ? 'bids' : 'asks';
    const oppositeSide = isBuy ? 'asks' : 'bids';

    // Keep keys as strings, sort by numeric value via Decimal
    const levelKeys = Object.keys(updatedBook[oppositeSide]).sort((a, b) =>
        isBuy ? new Decimal(a).cmp(new Decimal(b)) : new Decimal(b).cmp(new Decimal(a))
    );

    for (const levelKey of levelKeys) {
        if (remaining.lte(0)) break;

        const priceLevelDec = new Decimal(levelKey);

        // LIMIT price constraint with Decimal
        if (order.type === 'LIMIT') {
            const orderPriceDec = new Decimal(order.price);
            if (isBuy && orderPriceDec.lt(priceLevelDec)) break;
            if (!isBuy && orderPriceDec.gt(priceLevelDec)) break;
        }

        const queue = updatedBook[oppositeSide][levelKey];
        if (!queue || queue.length === 0) {
            delete updatedBook[oppositeSide][levelKey];
            continue;
        }

        while (queue.length > 0 && remaining.gt(0)) {
            const maker = queue.shift();
            const makerQtyDecimal = new Decimal(maker.qty);
            const matchQty = Decimal.min(remaining, makerQtyDecimal);

            const makerLeftover = makerQtyDecimal.minus(matchQty);
            const makerFilledAfter = makerLeftover.eq(0);
            const takerFilledAfter = remaining.minus(matchQty).eq(0);

            const trade = new Trade({
                symbol: order.symbol,
                maker: {
                    orderId: maker.orderId,
                    side: maker.side,
                    userId: maker.userId,
                    filled: makerFilledAfter,
                },
                taker: {
                    orderId: order.orderId,
                    side: order.side,
                    userId: order.userId,
                    filled: takerFilledAfter,
                },
                price: levelKey,                // keep original string key (e.g., "114882.00")
                qty: matchQty.toString(),
                timestamp: Date.now(),
            });
            trades.push(trade);

            remaining = remaining.minus(matchQty);

            if (makerLeftover.gt(0)) {
                maker.qty = makerLeftover.toString();
                queue.unshift(maker);
                break;
            }
        }

        if (queue.length === 0) {
            delete updatedBook[oppositeSide][levelKey];
        }
    }

    // LIMIT leftovers go to own side; MARKET leftovers are discarded
    if (order.type === 'LIMIT' && remaining.gt(0)) {
        const sideBook = updatedBook[ownSide];
        const key = order.price; // already a string
        if (!sideBook[key]) sideBook[key] = [];
        sideBook[key].push({
            orderId: order.orderId,
            side: order.side,
            type: order.type,
            price: order.price.toString(),
            qty: remaining.toString(),
            userId: order.userId,
        });
    }

    tradeCount += trades.length;
    return { trades, updatedBook };
}