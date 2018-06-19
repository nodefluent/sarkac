"use strict";

const EventEmitter = require("events");

class Sarkac extends EventEmitter {

    constructor(config = {}){
        super();

        this.config = config;
    }

    async analyse(){

    }

    close(){

    }
}

module.exports = Sarkac;