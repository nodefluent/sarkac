"use strict";

class FieldIdentifier {

    constructor(config = {}){
        this.config = config;
    }

    async analyseMessageSchema(message) {
        return [];
    }
}

module.exports = FieldIdentifier;