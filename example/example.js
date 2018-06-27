"use strict";

const {kafkaConfig, testTopic} = require("./config.js");
const Sarkac = require("./../index.js");

const config = {
    kafka: kafkaConfig,
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
        [testTopic]: {
            fields: {
                "sub.one": {
                    //windows: ["30s", "1m", "5m", "15m", "1h", "12h", "2d", "1w"]
                    windows: ["1m"]
                },
                "two": {
                    //windows: ["30s", "1m", "5m", "15m", "1h", "12h", "2d", "1w"]
                    windows: ["3m"]
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
        beforeMessageProcessing: (message, callback) => {
            try {
                message.key = message.key.toString("utf8");
                message.value = message.value.toString("utf8");
                message.value = JSON.parse(message.value);
                callback(null, {
                    topic: message.topic,
                    key: message.key,
                    value: message.value,
                    timestamp: message.timestamp || Date.now()
                });
            } catch(error){
                return callback(error);
            }
        },
        beforeAnomalyProduction: (message, callback) => { callback(null, message); }
    },
    anomalyScanMs: 15000,
    anomalyScanConcurrency: 2,
    discovery: {
        enabled: true,
        scanMs: 15000
    }
};

const sarkac = new Sarkac(config);

sarkac.on("anomaly", (anomaly) => console.log(anomaly));
sarkac.on("message", (message) => {/* empty */});
sarkac.on("error", (error) => console.error(error));

sarkac.analyse().catch((error) => console.error(error));
//setTimeout(sarkac.close.bind(sarkac), 25000);