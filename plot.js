import express from 'express';
import  http from 'http';
import { Server }from 'socket.io';
import fs from 'fs/promises'
import sqlite3 from 'better-sqlite3'

import path from 'path';

import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);



const app = express();
const PORT = process.env.PORT || 3000
const server = http.createServer(app);
const io = new Server(server,{
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    allowedHeaders: ["my-custom-header"],
    credentials: true
  }}
);

var colors = []

// app.get('/', (req, res) => {
//   res.send("<h1>Hello Krishnendu</h1>");
// });


app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

io.on('connection', (socket) => {
  console.log('A user connected');
  socket.on('message', (msg) => {
    console.log('Message received:', msg);
    io.emit('message', msg);
  });
  socket.on('disconnect', () => {
    console.log('A user disconnected');
  });

  socket.on("timeframe", (msg)=>{
    console.log(msg)
    timeframe = parseInt(msg,10)
  })

});

const db = {}
db["BTCUSDT"] = sqlite3('BTCUSDT.db')
db["ETHUSDT"] = sqlite3('ETHUSDT.db')

server.listen(PORT, () => {
  console.log(`Server is running on http://localhost:${PORT}`);
});



var today = new Date().toISOString().slice(0, 10);


function convertToOHLC(data, intervalSecond, timestamp_field, ltp_field, skip_second = 0) {
    return new Promise(function(res, rej) {
        const intervalMillis = intervalSecond * 1000;
        const skipMillis = skip_second * 1000;

        // Adjust all timestamps by adding skip seconds
        const adjustedData = data.map(entry => ({
            ...entry,
            [timestamp_field]: entry[timestamp_field] + skipMillis
        }));

        const grouped = adjustedData.reduce((acc, entry) => {
            var intervalStart = Math.floor(entry[timestamp_field] / intervalMillis) * intervalMillis;
            
            if (!acc[intervalStart]) acc[intervalStart] = [];
            acc[intervalStart].push(entry);
            return acc;
        }, {});

        const ohlcArray = Object.entries(grouped).map(([intervalStart, entries]) => {
            const open = entries[0][ltp_field];
            const close = entries[entries.length - 1][ltp_field];
            const high = Math.max(...entries.map(e => e[ltp_field]));
            const low = Math.min(...entries.map(e => e[ltp_field]));

            return {
                x: parseInt(intervalStart, 10) - skipMillis, // Subtract skip seconds here
                o: parseFloat(open.toFixed(2)),
                h: parseFloat(high.toFixed(2)),
                l: parseFloat(low.toFixed(2)),
                c: parseFloat(close.toFixed(2))
            };
        });

        res(ohlcArray);
    });
}

function get_divergence_colors(price_ohlc,cvd_ohlc){
  var background_colors = []
  var bullish_divergence1 = "#0000FF";
  var bearish_divergence1 = "#800080";
  var background_color = {}
  
  for(var i=0;i<price_ohlc.length;i++){
      background_color = {up:"#00FF00",down:"#FF0000"}
      
      if( price_ohlc[i].c>price_ohlc[i].o  && cvd_ohlc[i].c<cvd_ohlc[i].o ){
        background_color = {up:bullish_divergence1,down:bullish_divergence1}
       }

      if( price_ohlc[i].c<price_ohlc[i].o  && cvd_ohlc[i].c>cvd_ohlc[i].o  ){
        background_color = {up:bearish_divergence1,down:bearish_divergence1}
       }
      background_colors.push(background_color)
  }
  return background_colors
}

async function get_crypto_report(symbol){
  return new Promise(async function(res, rej){

  var sql = `SELECT A.TIMESTAMP_10*1000 AS TIMESTAMP_10, B.PRICE, A.CUMDELTA AS CUMDELTA FROM 
  (SELECT TIMESTAMP_10,AVG(CUMDELTA) AS CUMDELTA FROM TRADE GROUP BY TIMESTAMP_10) AS A,
  (SELECT TIMESTAMP_10,AVG(PRICE) AS PRICE FROM PRICE GROUP BY TIMESTAMP_10) AS B
  WHERE A.TIMESTAMP_10 = B.TIMESTAMP_10`
    var data = db[symbol].prepare(sql).all()

    var price_10_ohlc = await convertToOHLC(data, 600 , "TIMESTAMP_10" , "PRICE")
    var price_15_ohlc = await convertToOHLC(data, 900 , "TIMESTAMP_10" , "PRICE")
    var price_30_ohlc = await convertToOHLC(data, 1800 , "TIMESTAMP_10" , "PRICE")
    var cvd_10_ohlc = await convertToOHLC(data, 600 , "TIMESTAMP_10" , "CUMDELTA")
    var cvd_15_ohlc = await convertToOHLC(data, 900 , "TIMESTAMP_10" , "CUMDELTA")
    var cvd_30_ohlc = await convertToOHLC(data, 1800 , "TIMESTAMP_10" , "CUMDELTA")
    var divergence_30_colors = get_divergence_colors(price_30_ohlc,cvd_30_ohlc)
    var divergence_15_colors = get_divergence_colors(price_15_ohlc,cvd_15_ohlc)
    var divergence_10_colors = get_divergence_colors(price_10_ohlc,cvd_10_ohlc)

    res({price_10_ohlc,price_15_ohlc,price_30_ohlc,cvd_10_ohlc,cvd_15_ohlc,cvd_30_ohlc,divergence_30_colors,divergence_15_colors,divergence_10_colors})
  })

}

(async function(){

  var write_data = {}
  setInterval(async function(){


      var btcusdt_report = await get_crypto_report("BTCUSDT")
      var ethusdt_report = await get_crypto_report("ETHUSDT")
      // console.log(btcusdt_report)
      
      // write_data = {"BTCUSDT":btcusdt_report}

      io.emit("crypto",{"BTCUSDT":btcusdt_report, "ETHUSDT":ethusdt_report})
      console.log("Updating Data")
   },5*1000)

  setInterval(function(){
    // writeUserData(write_data)
  },15*1000)

})()