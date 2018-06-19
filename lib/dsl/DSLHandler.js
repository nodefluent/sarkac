"use strict";

const juration = require("juration");

class DSLHandler {

    constructor(dsl = {}, sarkac = null){
        this.dsl = dsl;
        this.mongo = sarkac.mongo;
        this.topics = [];
    }

    prepare(){
        this.topics = Object.keys(this.dsl);
        //TODO
    }

    getKafkaTopics(){
        return this.topics;
    }

    async handleMessage(message, stream){
        //TODO
        return null;
    }
}

module.exports = DSLHandler;