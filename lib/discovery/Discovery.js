"use strict";

const debug = require("debug")("sarkac:discovery");
const murmur = require("murmurhash");

const FieldIdentifier = require("./FieldIdentifier.js");

const DEFAULT_DISCOVER_MS = 15000;
const DEFAULT_DISCOVER_FIELDS_MS = 30000;

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

        this.schemaHashes = {};
        this.discoveredFields = [];
        this.fieldDiscoveries = {};
        this.lastFieldDiscoveryReset = null;

        //TODO: how can a discovered but deprecated setup be cleaned up e.g. deletion of a topic
    }

    _arrayToFixedHash(array){
        return murmur.v3(array.sort().join(":"));
    }

    async _discover(){

        try {
            await this.discoverTopics();
        } catch(error){
            debug("Discovery failed", error.message);
        }

        try {
            const timeout = this.config.discovery.fieldScanMs || DEFAULT_DISCOVER_FIELDS_MS;
            if(this.lastFieldDiscoveryReset === null || this.lastFieldDiscoveryReset + timeout < Date.now()){
                this.lastFieldDiscoveryReset = Date.now();

                // reset for all known topics, this will cause handleMessage to discover them on the next message
                this.discoveredTopics.forEach((topic) => {
                    this.fieldDiscoveries[topic] = false;
                });

                debug("Field discoveries have been reset.");
            }
        } catch(error){
            debug("Field discovery reset failed", error.message);
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

        let topics = await this.kafkaClient.getTopicList();

        if(!topics || !topics.length){
            debug("No topics discovered.");
            return false;
        }

        let blacklist = [];
        if(this.config.discovery && Array.isArray(this.config.discovery.topicBlacklist)){
            blacklist = JSON.parse(JSON.stringify(this.config.discovery.topicBlacklist));
        }

        // remove own anomaly topic
        if(this.config.target && this.config.target.produceAnomalies && this.config.target.topic){
            blacklist.push(this.config.target.topic);
        }

        topics = topics.filter((topic) => blacklist.indexOf(topic) === -1);

        const newTopicsHash = this._arrayToFixedHash(topics);

        if(this.lastTopicsHash === newTopicsHash){
            debug("Topic hashes are identical, no new topics discovered.");
            return false;
        }

        debug("Topic hashes have changed old:", this.lastTopicsHash, "new:", newTopicsHash, "discovered:", topics);
        this.lastTopicsHash = newTopicsHash;
        this.discoveredTopics = topics;

        this.sarkac.emit("discovered-topics", topics);
        return true;
    }

    async handleMessage(message){

        if(!this.isActive){
            return false;
        }

        if(!message.topic || typeof message.value === "undefined"){
            debug("Cannot identify anything if a message is missing topic or value");
            return false;
        }

        if(this.fieldDiscoveries[message.topic]){
            // has already been discovered in the last scan interval
            return false;
        }

        const fields = await this.fieldIdentifier.analyseMessageSchema(message);
        debug("detected fields", fields);
        this.fieldDiscoveries[message.topic] = true; // flag as discovered

        const newFieldsHash = this._arrayToFixedHash(fields);
        if(this.schemaHashes[message.topic] === newFieldsHash){
            debug("Fields hash is identical for topic", message.topic);
            return false;
        }

        debug("Fields hash changed, detected fields:", fields);
        this.schemaHashes[message.topic] = newFieldsHash;
        this.discoveredFields[message.topic] = fields;

        this.sarkac.emit("discovered-fields", message.topic, fields);
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