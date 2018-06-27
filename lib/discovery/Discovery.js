"use strict";

const debug = require("debug")("sarkac:discovery");
const murmur = require("murmurhash");

const FieldIdentifier = require("./FieldIdentifier.js");

const DEFAULT_DISCOVER_MS = 15000;

class Discovery {

    constructor(config, sarkac){
        this.config = config;
        this.sarkac = sarkac;
        this.kafkaClient = null;
        this.fieldIdentifier = new FieldIdentifier();
        this._scanTimeout = null;
        this.isActive = false;
        this.lastTopicsHash = null;
        this.discoveredTopics = [];
        //TODO: how can a discovered but deprecated setup be cleaned up e.g. deletion of a topic
    }

    async _discover(){

        try {
            await this.discoverTopics();
        } catch(error){
            debug("Discovery failed", error.message);
        }

        this._scanTimeout = setTimeout(this._discover.bind(this), this.config.discovery.scanMs || DEFAULT_DISCOVER_MS);
    }

    async start(){

        if(!this.config.discovery || !this.config.discovery.enabled){
            return debug("Discovery not running.");
        }

        debug("Discovery is configured, starting..");

        this.kafkaClient = this.sarkac.stream.getKafkaClient();

        this._discover().catch((error) => {
            debug("Failed to start discover process", error.message);
        });

        this.isActive = true;
    }

    async discoverTopics(){

        const topics = await this.kafkaClient.getTopicList();

        if(!topics || !topics.length){
            debug("No topics discovered.");
            return false;
        }

        const newTopicsHash = murmur.v3(topics.sort().join(""));

        if(this.lastTopicsHash === newTopicsHash){
            debug("Topic hashes are identical, no new topics discovered.");
            return false;
        }

        debug("Topic hashes have changed old:", this.lastTopicsHash, "new:", newTopicsHash, "discovered", topics.length, "topics");
        this.lastTopicsHash = newTopicsHash;

        this.discoveredTopics = topics;
        return true;
    }

    async handleMessage(message){

        if(!this.isActive){
            return false;
        }

        const fields = await this.fieldIdentifier.analyseMessageSchema(message);
        return true;
    }

    close(){

        debug("Closing..");
        if(this._scanTimeout){
            clearTimeout(this._scanTimeout);
        }
    }
}

module.exports = Discovery;