"use strict";

const EventEmitter = require("events");
const debug = require("debug")("sarkac:main");

const DSLHandler = require("./dsl/DSLHandler.js");
const MongoWrapper = require("./db/MongoWrapper.js");
const Stream = require("./Stream.js");
const Discovery = require("./discovery/Discovery.js");
const HttpServer = require("./HttpServer.js");

const DEFAULT_SCAN_MS = 15000;
const DEFAULT_SCAN_CONC = 2;

class Sarkac extends EventEmitter {

    constructor(config = {}){
        super();

        this.config = config;

        this.mongo = new MongoWrapper(this.config.mongo);
        this.dslHandler = new DSLHandler(this.config, this);
        this.stream = new Stream(this.config, this);
        this.discovery = new Discovery(this.config, this);
        this.httpServer = new HttpServer(this.config, this);

        this._analysisTimeout = null;
        this.stats = {
            scanRuns: 0,
            anomaliesDetected: 0,
            analysedMessages: 0,
            topicUpdates: 0,
            fieldUpdates: 0,
            errors: 0
        };
    }

    async analyse(){

        debug("Starting..");

        try {
            this.dslHandler.prepare();
        } catch(error){
            debug("Failed to process dsl", error.message);
            return false;
        }

        await this.mongo.start();
        await this.stream.start();
        await this.discovery.start(this.stream.getKafkaClient(), true);
        await this.httpServer.start();

        // subscribe to discovery events

        this.discovery.on("created-topics", (topics) => this.emit("created-topics", topics));
        this.discovery.on("deleted-topics", (topics) => this.emit("deleted-topics", topics));
        this.discovery.on("discovered-topics", (topics) => this.emit("created-topics", topics));
        this.discovery.on("discovered-fields", (topics) => this.emit("discovered-fields", topics));

        // process stats from events

        this.on("anomaly", (_) => this.stats.anomaliesDetected++);
        this.on("message", (_) => this.stats.analysedMessages++);
        this.on("discovered-topics", (_) => this.stats.topicUpdates++);
        this.on("discovered-fields", (_) => this.stats.fieldUpdates++);
        this.on("error", (_) => this.stats.errors++);

        this.runRecursiveAnalysis();
        debug("Running..");
    }

    runRecursiveAnalysis(){

        if(this.closed){
            return;
        }

        this.stats.scanRuns++;

        const selfBound = this.runRecursiveAnalysis.bind(this);

        if(!this.dslHandler || !this.dslHandler.isPrepared){
            this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            return;
        }

        this.dslHandler
            .analyseAndCacheForAllTopics(this.config.anomalyScanConcurrency || DEFAULT_SCAN_CONC)
            .then((results) => {
                debug("Successfully ran calculations for", results.length, "tasks");
                this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            })
            .catch((error) => {
                debug("Failed to run calculations", error.message);
                this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            });
    }

    async getStats(){
        return {
            stream: this.stream ? this.stream.getKafkaStats() : null,
            db: {
                storedEvents: this.mongo? await this.mongo.getModel("sigma").getTotalCountOfEvents() : null
            },
            sarkac: this.stats
        };
    }

    close(){

        debug("Closing..");
        if(this._analysisTimeout){
            clearTimeout(this._analysisTimeout);
        }

        this.httpServer.close();
        this.discovery.close();
        this.stream.close();
        this.mongo.close();
    }
}

module.exports = Sarkac;