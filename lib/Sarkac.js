"use strict";

const EventEmitter = require("events");
const debug = require("debug")("sarkac:main");

const DSLHandler = require("./dsl/DSLHandler.js");
const MongoWrapper = require("./db/MongoWrapper.js");
const RedisWrapper = require("./db/RedisWrapper.js");
const Stream = require("./Stream.js");
const Discovery = require("./discovery/Discovery.js");

const DEFAULT_SCAN_MS = 15000;
const DEFAULT_SCAN_CONC = 2;

class Sarkac extends EventEmitter {

    constructor(config = {}){
        super();

        this.config = config;
        this.redis = new RedisWrapper(this.config.redis);
        this.mongo = new MongoWrapper(this.config.mongo);
        this.dslHandler = new DSLHandler(this.config.dsl, this);
        this.stream = new Stream(this.config, this);
        this.discovery = new Discovery(this.config, this);
        this._analysisTimeout = null;
    }

    async analyse(){

        debug("Starting..");

        try {
            this.dslHandler.prepare();
        } catch(error){
            debug("Failed to process dsl", error.message);
            return false;
        }

        await Promise.all([
            this.mongo.start(),
            this.redis.start()
        ]);

        await this.stream.start();
        await this.discovery.start();
        this.runRecursiveAnalysis();
        debug("Running..");
    }

    runRecursiveAnalysis(){

        if(this.closed){
            return;
        }

        const selfBound = this.runRecursiveAnalysis.bind(this);

        if(!this.dslHandler || !this.dslHandler.isPrepared){
            this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            return;
        }

        this.dslHandler
            .analyseAndCacheForAllTopics(this.config.anomalyScanConcurrency || DEFAULT_SCAN_CONC)
            .then((results) => {
                debug("Successfully ran anomaly scan for", results.length, "tasks");
                this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            })
            .catch((error) => {
                debug("Failed to run anomaly scan", error.message);
                this._analysisTimeout = setTimeout(selfBound, this.config.anomalyScanMs || DEFAULT_SCAN_MS);
            });
    }

    close(){

        debug("Closing..");
        if(this._analysisTimeout){
            clearTimeout(this._analysisTimeout);
        }

        this.discovery.close();
        this.stream.close();
        this.mongo.close();
        this.redis.close();
    }
}

module.exports = Sarkac;