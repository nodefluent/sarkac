"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:dsl");
const juration = require("juration");

const {getByPath} = require("./../helper/index.js");

class DSLHandler {

    constructor(dsl = {}, sarkac = null){
        this.dsl = dsl;
        this.mongo = sarkac.mongo;
        this.topics = [];
        this.parsed = {};
    }

    prepare(){

        // read topics
        this.topics = Object.keys(this.dsl);

        // parse windows
        Object
            .keys(this.dsl)
            .map((topic) => Object.assign({}, this.dsl[topic], {topic}))
            .filter((topicConf) => {

                if(!topicConf.fields){
                    debug("Missing fields object for topic", topicConf);
                    return false;
                }

                return true;
            })
            .forEach((topicConf) => {

                const {
                    topic,
                    fields
                } = topicConf;

                this.parsed[topic] = [];

                Object
                    .keys(fields)
                    .forEach((fieldPath) => {

                        if(!fields[fieldPath].windows || !Array.isArray(fields[fieldPath].windows)){
                            debug("Failed to parse windows for field", fieldPath, "in topic", topicConf);
                            return;
                        }

                        if(!fields[fieldPath].windows.length){
                            debug("A window configuration for a field should have at least one window", fieldPath, "in topic", topicConf);
                            return;
                        }

                        const windows = fields[fieldPath].windows.map((stringDuration) => {
                            try {
                                return juration.parse(stringDuration)
                            } catch(error){
                                debug("Failed to parse duration of window", topicConf, stringDuration, error.message);
                                return null;
                            }
                        }).filter((value) => !!value);

                        this.parsed[topic].push({
                            path: fieldPath,
                            windows,
                            retentionSeconds: Math.max(...windows)
                        });
                    });
            });

        debug("Parsed dsl", JSON.stringify(this.parsed));
    }

    getKafkaTopics(){
        return this.topics;
    }

    async handleMessage(message, stream){

        const fields = this.parsed[message.topic];
        if(!fields){
            throw new Error("Cannot handle message of topic", message.topic, "as it is not configured");
        }

        const fieldHandlePromises = fields.map((field) => this.handleField(field, message, stream));
        await Promise.all(fieldHandlePromises);
    }

    async handleField(fieldConfig, message, stream){

        let value = null;
        try {
            value = getByPath(message.value, fieldConfig.path);
            if(typeof value === "undefined" || value === null){
                debug("Field for path in message is undefined or null", fieldConfig, message);
                return null;
            }
        } catch(error){
            debug("Failed to get value for path of field in message", fieldConfig, message, error.message);
            return null;
        }

        debug("Parsed value", fieldConfig.path, value);
        const sigmaModel = this.mongo.getModel("sigma");
        const startT = Date.now();

        //TODO: check if enough values are stored, before actually processing

        await sigmaModel.storeValueOfEvent(message.topic, fieldConfig.path, value, message.timestamp);
        //await sigmaModel.cleanValuesOfEventForRetention(message.topic, fieldConfig.path, fieldConfig.retentionSeconds);
        //TODO: handle multiple windows
        const median = await sigmaModel.getMedianForEventWindow(message.topic, fieldConfig.path, fieldConfig.windows[0]);
        const stdDev = await sigmaModel.getStdDevForEventWindow(message.topic, fieldConfig.path, fieldConfig.windows[0]);
        const threeSigma = (value - median) / (3 * stdDev);

        //TODO: check NaN

        const isAnomaly = threeSigma > 1.0 || threeSigma < - 1.0;

        debug("median", median, "stdDev", stdDev, "3-sigma", threeSigma, "is anomaly", isAnomaly);
        //TODO: produce anomaly detection to kafka
        //TODO: emit anomaly detection event

        const endT = Date.now();

        debug("Processed value for field", message.topic, fieldConfig.path, "took", endT - startT, "ms");
    }
}

module.exports = DSLHandler;