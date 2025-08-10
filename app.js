import WebSocket from 'ws';
import Database from 'better-sqlite3';

// Initialize SQLite database
const db = {}
db["BTCUSDT"] = new Database('BTCUSDT.db');
db["ETHUSDT"] = new Database('ETHUSDT.db');

db["BTCUSDT"].prepare(`CREATE TABLE IF NOT EXISTS PRICE (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TIMESTAMP NUMERIC,
    TIMESTAMP_10 NUMERIC,
    PRICE NUMERIC
)`).run();

db["BTCUSDT"].prepare(`CREATE TABLE IF NOT EXISTS TRADE (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TIMESTAMP NUMERIC,
    TIMESTAMP_10 NUMERIC,
    PRICE NUMERIC,
    QUANTITY NUMERIC,
    TRADE_TYPE TEXT,
    DELTA NUMERIC,
    CUMDELTA NUMERIC
)`).run();
db["ETHUSDT"].prepare(`CREATE TABLE IF NOT EXISTS PRICE (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TIMESTAMP NUMERIC,
    TIMESTAMP_10 NUMERIC,
    PRICE NUMERIC
)`).run();

db["ETHUSDT"].prepare(`CREATE TABLE IF NOT EXISTS TRADE (
    ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TIMESTAMP NUMERIC,
    TIMESTAMP_10 NUMERIC,
    PRICE NUMERIC,
    QUANTITY NUMERIC,
    TRADE_TYPE TEXT,
    DELTA NUMERIC,
    CUMDELTA NUMERIC
)`).run();

// db.prepare(`CREATE TABLE IF NOT EXISTS ORDER_BOOK (
//     ID INTEGER PRIMARY KEY AUTOINCREMENT,
//     TIMESTAMP NUMERIC,
//     TIMESTAMP_10 NUMERIC,
//     TOTAL_BIDS NUMERIC,
//     TOTAL_ASKS NUMERIC,
//     DIFFERENCE NUMERIC,
//     CUM_BID_ASK NUMERIC,
//     SPREAD NUMERIC
// )`).run();

// WebSocket URL for Binance futures
const wsUrlPrice = 'wss://fstream.binance.com/stream?streams=btcusdt@ticker/ethusdt@ticker';
const wsUrlTrade = 'wss://fstream.binance.com/stream?streams=btcusdt@trade/ethusdt@trade';
const wsUrlOrderBook = 'wss://fstream.binance.com/v1/ws/btcusdt@depth20@100ms';

// Function to handle WebSocket connection
async function binanceFuturesPriceWS() {
    const ws = new WebSocket(wsUrlPrice);

    ws.on('open', () => {
        console.log('WebSocket connection established.');
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message).data;
            // console.log(data)
            const symbol = data.s
            const timestamp = Math.floor(data.E / 1000); // Convert to seconds
            const timestamp_10 = timestamp - (timestamp % 10);
            const price = parseFloat(data.c); // 'c' refers to the current price in the ticker stream
            console.log(price)
            if (price > 0) {
                const stmt = db[symbol].prepare('INSERT INTO PRICE (TIMESTAMP, TIMESTAMP_10, PRICE) VALUES (?, ?, ?)');
                stmt.run(timestamp, timestamp_10, price);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    });

    ws.on('close', () => {
        console.log('WebSocket connection closed. Retrying...');
        setTimeout(binanceFuturesWS, 5000); // Retry connection after 5 seconds
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
}

var cumdelta = 0;

async function binanceFuturesTradeWS() {
    const ws = new WebSocket(wsUrlTrade);


    ws.on('open', () => {
        console.log('WebSocket connection established.');
    });

    ws.on('message', (message) => {
        try {

            const tradeData = JSON.parse(message).data;
            // console.log(tradeData)
            const symbol = tradeData.s
            const tradeId = tradeData.t;
            const price = parseFloat(tradeData.p);
            const quantity = parseFloat(tradeData.q);
            const isBuyerMaker = tradeData.m;
            const timestamp = Math.floor(tradeData.T / 1000); // Convert to seconds
            const timestamp_10 = timestamp - (timestamp % 10);

            let tradeType;
            let delta;

            if (isBuyerMaker) {
                tradeType = "Sell";
                delta = -quantity;
            } else {
                tradeType = "Buy";
                delta = quantity;
            }

            cumdelta += delta;

            if (price > 0) {
                const stmt = db[symbol].prepare('INSERT INTO TRADE (TIMESTAMP, TIMESTAMP_10, PRICE, QUANTITY, TRADE_TYPE, DELTA, CUMDELTA) VALUES (?, ?, ?, ?, ?, ?, ?)');
                stmt.run(timestamp, timestamp_10, price, quantity, tradeType, delta, cumdelta);
            }
        } catch (error) {
            console.error('Error processing trade data:', error);
        }
    });

    ws.on('close', () => {
        console.log('WebSocket connection closed. Retrying...');
        setTimeout(tradeHandler, 5000); // Retry connection after 5 seconds
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
    });
}

var cumBidAsk = 0;
async function binanceFuturesOrderBookWS() {
    const ws = new WebSocket(wsUrlOrderBook);
    

    ws.on('open', () => {
        console.log("Connected to Binance WebSocket");
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            const timestamp = Math.floor(data.E / 1000); // Convert to seconds
            const timestamp_10 = timestamp - (timestamp % 10);

            const bids = data.b.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)]);
            const asks = data.a.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)]);

            const totalBids = bids.reduce((sum, [, quantity]) => sum + quantity, 0);
            const totalAsks = asks.reduce((sum, [, quantity]) => sum + quantity, 0);
            const spread = asks[0][0]-bids[0][0]
            // console.log(bids[0])

            const difference = totalBids - totalAsks;
            cumBidAsk += difference;

            const stmt = db.prepare('INSERT INTO ORDER_BOOK (TIMESTAMP, TIMESTAMP_10, TOTAL_BIDS, TOTAL_ASKS, DIFFERENCE, CUM_BID_ASK, SPREAD) VALUES (?, ?, ?, ?, ?, ?,?)');
            stmt.run(timestamp, timestamp_10, totalBids, totalAsks, difference, cumBidAsk,spread);
        } catch (error) {
            console.error("Error processing order book data:", error);
        }
    });

    ws.on('close', () => {
        console.log("WebSocket connection closed, retrying...");
        setTimeout(orderBookStream, 5000); // Retry connection after 5 seconds
    });

    ws.on('error', (error) => {
        console.error("WebSocket error:", error);
    });
}


// Start the WebSocket connection
binanceFuturesPriceWS();
binanceFuturesTradeWS();
// binanceFuturesOrderBookWS();
