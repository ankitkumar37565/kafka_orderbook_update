const { Kafka } = require("kafkajs");
const ws = require("ws");
const axios = require("axios");
const events = require("events");
const { error } = require("console");
const orderbook = {
    bids: {},
    asks: {},
    U: null, //first update Id
    u: null, //final update Id
};
let orderbookEmitter = new events.EventEmitter();
const socketService = {
    ws_orderBookService: async function () {
        try {
            // await socketService.depthSnapshot();
            console.log("bids: ", Object.keys(orderbook.bids).length, " asks: ", Object.keys(orderbook.asks).length);
            let connection = new ws("wss://fstream.binance.com/stream?streams=btcusdt@depth@100ms");
            connection.on("open", () => {
                console.log("ws_orderBookService socket connection open ");
            });
            connection.on("message", async (message) => {
                // message=JSON.stringify(message,null,4)
                message = JSON.parse(message);
                // console.log("message", message);
                if (message.data.e == "depthUpdate" && (!message.data.pu || !message.data.U || !message.data.u)) {
                    if (orderbook.u && orderbook.u !== message.data.u) connection.close();
                } else {
                    orderbookEmitter.emit("orderbook_update", message.data);
                    //  let bids_present=0,asks_present=0,bids_total=0,asks_total=0
                    // if (message.data.b) {
                    //     message.data.b.forEach((element) => {
                    //         bids_total++;
                    //         if(orderbook.bids[element[0]]) bids_present++;
                    //         orderbook.bids[element[0]] = element[1];
                    //     });
                    // }
                    // if (message.data.a) {
                    //     message.data.a.forEach((element) => {
                    //         asks_total++;
                    //         if(orderbook.asks[element[0]]) asks_present++;
                    //         orderbook.asks[element[0]] = element[1];
                    //     });
                    // }
                    // orderbook.U = message.data.U;
                    // orderbook.u = message.data.u;
                    // console.log(bids_total,bids_present,asks_total,asks_present)
                }

                // console.log('message',message)
            });
            connection.on("ping", () => {
                connection.pong();
            });
            connection.on("close", socketService.ws_orderBookService);
            connection.on("error", (error) => {
                console.log("orderbook socket connection error :", error);
            });
        } catch (error) {
            console.log("error @ ws_orderBookService", error);
        }
    },
    sleep: async function (ms) {
        return new Promise((resolve) => {
            setTimeout(resolve, ms);
        });
    },
    depthSnapshot: async function () {
        try {
            let response = await axios({
                method: "GET",
                url: "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000",
            });
            if (response.status == 200 && response.data && response.data.bids && response.data.asks) {
                // console.log('response',response.data)

                response.data.bids.forEach((element) => {
                    orderbook.bids[element[0]] = element[1];
                });
                response.data.asks.forEach((element) => {
                    orderbook.asks[element[0]] = element[1];
                });
                orderbook.u = response.data.lastUpdateId;
                console.log("orderbook initiated");
            }
        } catch (error) {
            console.log("error @ depthSnapshot :", error);
        }
    },
};
const kafka = new Kafka({
    clientId: "kafkaclient1",
    brokers: ["127.0.0.1:9092"],
});
let run = async function () {
const producer = kafka.producer();
await producer.connect();

orderbookEmitter.on("orderbook_update", async (data) => {
    console.log('new update')
    await producer.send({
        topic: 'orderbook_update',
        messages: [
            {value:JSON.stringify(data)}
        ],
    });
});
};
// setInterval(send,5000)
// setInterval(() => {
//     console.log(orderbook)
//     console.log("bids: ", Object.keys(orderbook.bids).length, " asks: ", Object.keys(orderbook.asks).length);
// }, 5000);

socketService.ws_orderBookService().then(run().catch(console.error())).catch(console.error());
