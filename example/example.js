"use strict";

const {kafkaConfig} = require("./kafkaConfig.js");
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
    // optional, if discovery is not used
    /* dsl: {
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
    }, */
    target: {
        produceAnomalies: true,
        topic: "sarkac-detected-anomalies",
        partitions: 1
    },
    hooks: {
        beforeMessageProcessing: (_message, callback) => {

            try {

                const message = {};
                message.key = _message.key.toString("utf8");
                message.value = JSON.parse(_message.value.toString("utf8"));
                message.timestamp = _message.timestamp || Date.now();
                message.topic = _message.topic;

                // these fields are mandatory!
                // if you do not provide them, you will have a bad time

                // fake async
                process.nextTick(() => {
                    callback(null, message);
                });

            } catch(error){

                // could also pass original message here
                // return callback(null, _message);

                // could also pass error here
                // return callback(error);

                // filter out errored hook handles by passing null
                return callback(null, null);
            }
        },
        beforeAnomalyProduction: (message, callback) => { callback(null, message); },
        //beforeDiscoveryFieldConfig: (topic, field, callback) => { callback(null, {windows: ["3m"]}); }
    },
    http: {
        enabled: true,
        port: 8033
    },
    anomalyScanMs: 15000,
    anomalyScanConcurrency: 2,
    discovery: {
        enabled: true,
        scanMs: 15000,
        fieldScanMs: 30000,
        defaultWindows: ["3m"],
        topicBlacklist: [] // target.topic and __consumer_offsets is added automatically
    },

    // optional
    freqAnomalyDetectionActive: true,
    freqFieldName: "__topic_frequency",
    freqWindows: ["15m", "12h", "1d"],
};

const sarkac = new Sarkac(config);

sarkac.on("anomaly", (anomaly) => console.log(anomaly));
sarkac.on("error", (error) => console.error(error));

/* further misc events you can subscribe to */
sarkac.on("message", (message) => {});
sarkac.on("discovered-topics", (topics) => {});
sarkac.on("created-topics", (topics) => {});
sarkac.on("deleted-topics", (topics) => {});
sarkac.on("discovered-fields", (topic, fields) => {});

sarkac.analyse().catch((error) => console.error(error));
//setTimeout(sarkac.close.bind(sarkac), 25000);