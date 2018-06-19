"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:stream");
const {KafkaStreams} = require("kafka-streams");

class Stream {

    constructor(baseConfig = {}, sarkac = null){
        this.config = baseConfig;
        this.sarkac = sarkac;
        this.dslHandler = sarkac.dslHandler;
        this.streams = new KafkaStreams(this.config.kafka);
        this.inputStream = null;
        this.outputStream = null;
    }

    async start(){

        debug("Loading DSL..");

        this.inputStream = this.streams
            .getKStream()
            .from(this.dslHandler.getKafkaTopics());

        debug("Consuming", this.dslHandler.getKafkaTopics().join(", "), "topics");

        if(this.config.hooks && this.config.hooks.beforeMessageProcessing &&
            typeof this.config.hooks.beforeMessageProcessing === "function"){

            this.inputStream = this.inputStream
                .map((message) => {
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
                })
                .awaitPromises()
                .filter((value) => value !== null);

            debug("Added beforeMessageProcessing hook.");
        } else {
            throw new Error("Missing config.hooks.beforeMessageProcessing hook. Should be (msg, cb) => {cb(msg);}.");
        }

        this.inputStream
            .map((message) => {
                this.sarkac.emit("message", message);
                return this.dslHandler.handleMessage(message, this).catch((error) => {
                    debug("Error while processing message", error.message);
                    this.sarkac.emit("error", error);
                    return null;
                });
            })
            .awaitPromises()
            .forEach(() => {/* drain */});

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

        await this.inputStream.start();

        debug("Done.");
    }

    produceMessage(message){

        if(this.outputStream){
            this.outputStream.writeToStream(message);
            return true;
        }

        return false;
    }

    close(){
        debug("Closing..");
        this.streams.closeAll();
    }
}

module.exports = Stream;