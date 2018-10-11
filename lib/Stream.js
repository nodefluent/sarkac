"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:stream");
const {NConsumer} = require("sinek");
const {KafkaStreams} = require("kafka-streams");

class Stream {

    constructor(baseConfig = {}, sarkac = null){
        this.config = baseConfig;
        this.sarkac = sarkac;
        this.dslHandler = sarkac.dslHandler;
        this.streams = new KafkaStreams(this.config.kafka);
        this.consumer = null;
        this.outputStream = null;
    }

    /**
     * Starts a
     * 1. native consumer to handle back pressure for mongo writes
     * 2. a kafka streams producer instance to stream results onto an anomaly topic, if configured
     * @returns {Promise<void>}
     */
    async start(){

        debug("Loading DSL..");

        const topics = this.dslHandler.getKafkaTopics();
        this.consumer = new NConsumer(topics, this.config.kafka);

        const processingMessageMapper = (message) => {
            this.sarkac.emit("message", message);
            return this.dslHandler.handleMessage(message, this).catch((error) => {
                debug("Error while processing message", error.message);
                this.sarkac.emit("error", error);
                return null;
            });
        };

        debug("Consuming", topics.join(", "), "topics");

        let incomingMessageMapper = (message) => { return Promise.resolve(message); };
        if(this.config.hooks && this.config.hooks.beforeMessageProcessing &&
            typeof this.config.hooks.beforeMessageProcessing === "function"){

            incomingMessageMapper = (message) => {
                    return new Promise((resolve) => {
                        this.config.hooks.beforeMessageProcessing(message, (error, altered) => {

                           if(error){
                               this.sarkac.emit("error", error);
                               debug("Error in beforeMessageProcessing hook", error.message);
                               return resolve(null);
                           }

                           resolve(altered);
                        });
                    });
                };

            debug("Added beforeMessageProcessing hook.");
        } else {
            throw new Error("Missing config.hooks.beforeMessageProcessing hook. Should be (msg, cb) => {cb(msg);}.");
        }

        if(this.config.target && this.config.target.produceAnomalies){
            if(this.config.target.topic && this.config.target.partitions){

                this.outputStream = this.streams.getKStream();

                if(this.config.hooks && this.config.hooks.beforeAnomalyProduction &&
                    typeof this.config.hooks.beforeAnomalyProduction === "function"){

                    this.outputStream = this.outputStream
                        .map((message) => {
                            return new Promise((resolve) => {
                                this.config.hooks.beforeAnomalyProduction(message, (error, altered) => {

                                    if(error){
                                        debug("Error in beforeAnomalyProduction hook", error.message);
                                        this.sarkac.emit("error", error);
                                        return resolve(null);
                                    }

                                    // if is formatted as kafka message object, pass through (kafka streams handles)
                                    if(typeof altered === "object" && typeof altered.key !== "undefined"
                                        && typeof altered.value !== "undefined"){
                                        return resolve(altered);
                                    }

                                    // if not, make sure the value is a string or a buffer before passing
                                    if(typeof altered !== "string" && !Buffer.isBuffer(altered)){
                                        altered = JSON.stringify(altered);
                                    }

                                    resolve(altered);
                                });
                            });
                        })
                        .awaitPromises()
                        .filter((value) => value !== null);

                    debug("Added beforeAnomalyProduction hook.");
                } else {
                    debug("No beforeAnomalyProduction hook function provided.");
                }

                this.outputStream.
                    to(this.config.target.topic, this.config.target.partitions, "send");
                debug("Producing to target topic", this.config.target.topic, "with", this.config.target.partitions, "partitions.");
            } else {
                throw new Error("Missing config.target.topic or config.target.partitions");
            }
        }

        debug("Done. Connecting to Kafka..");

        if(this.outputStream){
            await this.outputStream.start();
        }

        await this.consumer.connect();
        this.consumer.consume(async (message, callback) => {
            try {
                message = await incomingMessageMapper(message);

                // drop null messages
                if(message === null){
                    return;
                }

                await Promise.all([
                    await this.sarkac.discovery.handleMessage(message),
                    await processingMessageMapper(message)
                ]);
                callback(null);
            } catch(error){
                debug("Failed to process kafka message.", error);
                callback(error);
            }
        }, false, false, this.config.kafka.batchOptions);

        // register for topic updates
        this.sarkac.on("discovered-topics", (topics) => {
            debug("Topics changed, adusting consumer settings.");

            if(this.consumer) {
                this.consumer.adjustSubscription(topics);
                debug("Adjusted topics");
            }
        });

        debug("Done.");
    }

    produceMessage(anomaly = {}){

        if(this.outputStream){

            this.outputStream.writeToStream({
                key: anomaly.id,
                value: JSON.stringify(anomaly)
            });

            return true;
        }

        return false;
    }

    getKafkaClient(){
        return this.consumer;
    }

    getKafkaStats(){

        if(!this.consumer || !this.outputStream || !this.outputStream.kafka){
            return {
                consumer: null,
                producer: null
            };
        }

        return {
            consumer: this.consumer.getStats(),
            producer: this.outputStream.kafka.getStats()
        };
    }

    async close(){
        debug("Closing..");
        await this.consumer.close();
        await this.streams.closeAll();
    }
}

module.exports = Stream;