"use strict";

const EventEmitter = require("events");
const debug = require("debug")("sarkac:main");

const DSLHandler = require("./dsl/DSLHandler.js");
const MongoWrapper = require("./db/MongoWrapper.js");
const RedisWrapper = require("./db/RedisWrapper.js");
const Stream = require("./Stream.js");

class Sarkac extends EventEmitter {

    constructor(config = {}){
        super();

        this.config = config;
        this.redis = new RedisWrapper(this.config.redis);
        this.mongo = new MongoWrapper(this.config.mongo);
        this.dslHandler = new DSLHandler(this.config.dsl, this);
        this.stream = new Stream(this.config, this);
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
        debug("Running..");
    }

    close(){
        debug("Closing..");
        this.stream.close();
        this.mongo.close();
        this.redis.close();
    }
}

module.exports = Sarkac;