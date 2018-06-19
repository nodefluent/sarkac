"use strict";

const debug = require("debug")("sarkac:example:generator");
const {KafkaStreams} = require("kafka-streams");

const {kafka} = require("./config.js");
const streams = new KafkaStreams(kafka);

const stream = streams.getKStream();
stream.to("test-topic", 1, "send");

stream.start().then(() => {
   setInterval(() => {
       debug("Writing to stream..");
       stream.writeToStream(JSON.stringify({
           sub: {
               one: 15.5
           },
           two: 16
       }))
   }, 1000);
});