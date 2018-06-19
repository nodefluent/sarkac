"use strict";

const debug = require("debug")("sarkac:redis");
const Redis = require("ioredis");

class RedisWrapper {

    constructor(config = {}){
        this.config = config;
        this.redis = new Redis(this.config);
    }

    async start(){
        debug("Connecting..");
        const info = await this.redis.info();
        debug("Connected.");
        return !!info;
    }

    close(){
        this.redis.disconnect();
    }
}

module.exports = RedisWrapper;