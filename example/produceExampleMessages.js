"use strict";

const debug = require("debug")("sarkac:example:generator");
const { KafkaStreams } = require("kafka-streams");

const { kafkaConfig, testTopic } = require("./kafkaConfig.js");
const streams = new KafkaStreams(kafkaConfig);

const stream = streams.getKStream();
stream.to(testTopic, 1, "send");

stream.start().then(() => {

    // basic event every 2.5 seconds
   setInterval(() => {
       debug("Writing basic to stream..");
       stream.writeToStream(JSON.stringify({
           sub: {
               one: 15.5
           },
           two: 16
       }))
   }, 2500);

    // anomaly every 30 seconds
    setInterval(() => {
        debug("Writing one anomaly to stream..");
        stream.writeToStream(JSON.stringify({
            sub: {
                one: 150.5
            },
            two: 16
        }))
    }, 30000);

    // anomaly every 60 seconds
    setInterval(() => {
        debug("Writing two anomaly to stream..");
        stream.writeToStream(JSON.stringify({
            sub: {
                one: 15.5
            },
            two: -100
        }))
    }, 60000);
});