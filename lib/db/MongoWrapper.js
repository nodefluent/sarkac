"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:mongo");
const mongoose = require("mongoose");
const Schema = mongoose.Schema;

const SigmaModel = require("./model/SigmaModel.js");

class MongoWrapper {

    constructor(config = {}){
        this.config = config;
        this.models = {};
    }

    _connect(){
        debug("Connecting..");
        return new Promise((resolve) => {

            mongoose.set("bufferCommands", false);
            mongoose.Promise = Promise;
            mongoose.connect(this.config.url, this.config.options);
            const db = mongoose.connection;

            db.on("error", (error) => {
                debug("Error occured", error);
            });

            db.once("open", () => {
                debug("Connected.");
                resolve(this);
            });
        });
    }

    async start(){

        await this._connect();

        const sigmaModel = new SigmaModel();
        sigmaModel.registerModel(mongoose, Schema);
        this.models[sigmaModel.name] = sigmaModel;

        return true;
    }

    getModel(name) {
        return this.models[name];
    }

    close(){
        if(mongoose.connection){
            mongoose.connection.close(() => {});
        }
    }
}

module.exports = MongoWrapper;