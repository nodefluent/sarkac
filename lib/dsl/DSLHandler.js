"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:dsl");
const juration = require("juration");
const async = require("async");
const murmur = require("murmurhash");
const inMemoryCache = require("memory-cache");

const {getByPath} = require("./../helper/index.js");

const DEFAULT_FIELD_WINDOWS = ["15m"];
const DEFAULT_ANOMALY_RE_EMIT_COOLDOWN_MS = 2 * 60 * 1000; // 2 Minutes

class DSLHandler {

    constructor(config = {}, sarkac = null){
        this.config = config;
        // ensure clone
        this.dsl = config.dsl ? JSON.parse(JSON.stringify(config.dsl)) : {};
        this.sarkac = sarkac;
        this.mongo = sarkac.mongo;
        this.topics = [];
        this.parsed = {};
        this.cache = {};
        this.isPrepared = false;
    }

    _parseDSL(){

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

    prepare(){

        this._parseDSL();
        this.isPrepared = true;

        // listen for discovered field events
        this.sarkac.on("discovered-fields", (topic, fields) => {
            this.processDSLFieldsUpdate(topic, fields).catch((error) => {
                debug("Failed to process dsl fields update", error.message);
            });
        });
    }

    getKafkaTopics(){
        return this.topics;
    }

    async handleMessage(message, stream){

        if(!message.topic || typeof message.value === "undefined"){
            debug("Cannot handle message if its missing topic or value", message);
            return false;
        }

        const fields = this.parsed[message.topic];
        if(!fields){
            debug("Cannot handle message of topic:", message.topic, ", as it is not configured yet");
            return false;
        }

        // no useable fields detected
        if(!fields.length){
            return false;
        }

        const fieldHandlePromises = fields.map((field) => this.handleField(field, message, stream));
        await Promise.all(fieldHandlePromises);
        return true;
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

        // store value (this should be the only db operation running to handle a message)
        const sigmaModel = this.mongo.getModel("sigma");
        await sigmaModel.storeValueOfEvent(message.topic, fieldConfig.path, value, message.timestamp);

        // analyse all windows for the field in this message using the cached db means and stdDevs
        const handleTasks = fieldConfig.windows.map((window) => {
            return this.handleFieldWindow(value, message, fieldConfig, window, stream);
        });

        return Promise.all(handleTasks);
    }

    async handleFieldWindow(value, message, fieldConfig, window, stream){

        const cacheKey = `${message.topic}:${fieldConfig.path}:${window}`;

        if(!this.cache[cacheKey]){
            return null;
        }

        const {
            median,
            stdDev
        } = this.cache[cacheKey];

        const threeSigma = (value - median) / (3 * stdDev);
        const isAnomaly = threeSigma > 1.0 || threeSigma < -1.0;

        if(threeSigma === Infinity || threeSigma === -Infinity){
            debug("3-sigma is infinity.");
            return null;
        }

        if(isAnomaly){

            const inMemoryCacheKey = murmur.v3(cacheKey);
            if(inMemoryCache.get(inMemoryCacheKey)){

                debug("topic", message.topic, "for path", fieldConfig.path, "with value", value,
                    "median", median, "stdDev", stdDev, "3-sigma", threeSigma, "is anomaly, but wont emit, as we are in cooldown.");

                return false;
            }

            inMemoryCache.put(inMemoryCacheKey, true, DEFAULT_ANOMALY_RE_EMIT_COOLDOWN_MS);

            debug("topic", message.topic, "for path", fieldConfig.path, "with value", value,
                "median", median, "stdDev", stdDev, "3-sigma", threeSigma, "is anomaly");

            const anomalyId = murmur.v3(`${cacheKey}:${message.timestamp}`);

            const anomaly = {
                id:anomalyId,
                path: fieldConfig.path,
                window,
                humanWindow: juration.stringify(window),
                value,
                median,
                stdDev,
                threeSigma,
                originalMessage: message
            };

            this.sarkac.emit("anomaly", anomaly);
            await stream.produceMessage(anomaly)
        }

        return isAnomaly;
    }

    async analyseAndCacheField(topic, fieldConfig, window){

        const cacheKey = `${topic}:${fieldConfig.path}:${window}`;

        const sigmaModel = this.mongo.getModel("sigma");
        await sigmaModel.cleanValuesOfEventForRetention(topic, fieldConfig.path, fieldConfig.retentionSeconds);

        const eventCount = await sigmaModel.getCountOfAvailableEventsInWindow(topic, fieldConfig.path, window);
        if(!eventCount || eventCount < 3){

            debug("Not enough data stored in database to calculate 3 sigma", topic, fieldConfig.path, window, fieldConfig.retentionSeconds);

            if(this.cache[cacheKey]){
                debug("Cache present, clearing for", topic, fieldConfig.path, window);
                delete this.cache[cacheKey];
            }

            return false;
        }

        const median = await sigmaModel.getMedianForEventWindow(topic, fieldConfig.path, window);
        const stdDev = await sigmaModel.getStdDevForEventWindow(topic, fieldConfig.path, window);

        if(!median || !stdDev){
            debug("No data in database to calculate 3 sigma", topic, fieldConfig.path, window, fieldConfig.retentionSeconds);
            return false;
        }

        this.cache[cacheKey] = {
            median,
            stdDev
        };

        return true;
    }

    analyseAndCacheForAllTopics(concurrency = 1){

        const tasks = [];
        const startT = Date.now();

        // for window for field for topic into tasks array
        this.topics.forEach((topic) => {
            this.parsed[topic].forEach((field) => {
                field.windows.forEach((window) => {
                    tasks.push({
                        topic,
                        fieldConfig: field,
                        window
                    });
                });
            });
        });

        debug("Identified", tasks.length, "tasks for windows to analyse");

        // promise -> callback
        const iteratee = (task, callback) => {
            this.analyseAndCacheField(task.topic, task.fieldConfig, task.window).then((result) => {
                callback(null, result);
            }).catch((error) => {
                callback(error);
            });
        };

        // callback -> promise
        return new Promise((resolve, reject) => {
            async.mapLimit(tasks, concurrency, iteratee, (error, results) => {

                if(error){
                    return reject(error);
                }

                const endT = Date.now();
                debug("Processed total of", tasks.length, "took", endT - startT, "ms");
                resolve(results);
            });
        });
    }

    async processDSLFieldsUpdate(topic, paths){

        const buildDSL = {};
        await Promise.all(paths.map(async (path) => {
            try {
                buildDSL[path] = await this._handleSingleDSLFieldUpdate(topic, path);
                return true;
            } catch(error){
                debug("Failed to handle dsl field update", topic, path, error.message);
                return false;
            }
        }));

        const configDSL = this.config.dsl || {};
        if(configDSL[topic]){
            Object.keys(configDSL[topic]).forEach((path) => {
                // ensure that a fixed config always overwrites a discovered field
                buildDSL[path] = configDSL[topic][path];
            });
        }

        // overwrite the topics stored dsl and re-parse for the next message handle
        this.dsl[topic] = {
            fields: buildDSL
        };

        this._parseDSL();
    }

    async _handleSingleDSLFieldUpdate(topic, path){

        const {
            discovery,
            hooks
        } = this.config;

        // if there is a hook present, get right out with the hook
        if(hooks && typeof hooks.beforeDiscoveryFieldConfig === "function"){
            return await new Promise((resolve, reject) => {
                hooks.beforeDiscoveryFieldConfig(topic, path, (error, fieldConfig) => {

                    if(error){
                        return reject(error);
                    }

                    resolve(fieldConfig);
                });
            });
        }

        return {
            windows: discovery.defaultWindows || DEFAULT_FIELD_WINDOWS
        };
    }
}

module.exports = DSLHandler;