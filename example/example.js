"use strict";

const Promise = require("bluebird");
const Sarkac = require("./../index.js");

const config = {
    kafka: {
        noptions: {
            "metadata.broker.list": "localhost:9092",
            "group.id": "sarkac-example-group",
            "client.id": "sarkac-example-id",
            "event_cb": true,
            "compression.codec": "snappy",
            "api.version.request": true,
            "socket.keepalive.enable": true,
            "socket.blocking.max.ms": 100,
            "enable.auto.commit": false,
            "auto.commit.interval.ms": 100,
            "heartbeat.interval.ms": 250,
            "retry.backoff.ms": 250,
            "fetch.min.bytes": 100,
            "fetch.message.max.bytes": 2 * 1024 * 1024,
            "queued.min.messages": 100,
            "fetch.error.backoff.ms": 100,
            "queued.max.messages.kbytes": 50,
            "fetch.wait.max.ms": 1000,
            "queue.buffering.max.ms": 1000,
            "batch.num.messages": 10000
        },
        tconf: {
            "auto.offset.reset": "earliest",
            "request.required.acks": 1
        },
        batchOptions: {
            batchSize: 5,
            commitEveryNBatch: 1,
            concurrency: 1
        }
    },
    mongo: {
        url: "mongodb://localhost:27017/sarkac_example",
        options: {
            keepAlive: 120,
            autoIndex: true,
            reconnectTries: Number.MAX_VALUE,
            reconnectInterval: 500,
            poolSize: 20,
            bufferMaxEntries: 0,
            promiseLibrary: Promise
        }
    },
    redis: {
        host: "localhost",
        port: 6379,
        family: 4,
        db: 0,
        keyPrefix: "sarkac:"
    },
    hooks: {
        beforeMessageProcessing: (message, callback) => { callback(null, message); },
        beforeAnomalyProduction: (message, callback) => { callback(null, message); }
    },
    dsl: {
        "test-topic": {
            fields: {
                "sub.one": {
                    windows: ["1m", "15m", "1h", "12h", "2d", "1w"]
                }
            }
        }
    }
};

const sarkac = new Sarkac(config);

sarkac.on("anomaly", (anomaly, message) => console.log(anomaly, message));
sarkac.on("message", (message) => {/* empty */});
sarkac.on("error", (error) => console.error(error));

sarkac.analyse().catch((error) => console.error(error));
setTimeout(sarkac.close, 25000);