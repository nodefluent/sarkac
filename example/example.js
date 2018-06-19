"use strict";

const {kafka} = require("./config.js");
const Sarkac = require("./../index.js");

const config = {
    kafka,
    mongo: {
        url: "mongodb://localhost:27017/sarkac_example",
        options: {
            keepAlive: 120,
            autoIndex: true,
            reconnectTries: Number.MAX_VALUE,
            reconnectInterval: 500,
            poolSize: 20
        }
    },
    redis: {
        host: "localhost",
        port: 6379,
        family: 4,
        db: 0,
        keyPrefix: "sarkac:"
    },
    dsl: {
        "test-topic": {
            fields: {
                "sub.one": {
                    windows: ["1m", "15m", "1h", "12h", "2d", "1w"]
                }
            }
        }
    },
    target: {
        produceAnomalies: true,
        topic: "sarkac-detected-anomalies",
        partitions: 1
    },
    hooks: {
        beforeMessageProcessing: (message, callback) => { callback(null, message); },
        beforeAnomalyProduction: (message, callback) => { callback(null, message); }
    }
};

const sarkac = new Sarkac(config);

sarkac.on("anomaly", (anomaly, message) => console.log(anomaly));
sarkac.on("message", (message) => console.log(message));
sarkac.on("error", (error) => console.error(error));

sarkac.analyse().catch((error) => console.error(error));
//setTimeout(sarkac.close.bind(sarkac), 25000);