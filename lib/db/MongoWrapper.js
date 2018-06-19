"use strict";

const Promise = require("bluebird");
const debug = require("debug")("sarkac:mongo");
const mongoose = require("mongoose");
const Schema = mongoose.Schema;
const ObjectId = mongoose.Types.ObjectId;

class MongoWrapper {

    constructor(config = {}){
        this.config = config;
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

        //TODO
        const model = {};
        const schema = new Schema(model);
        mongoose.model("123", schema);
    }

    close(){
        if(mongoose.connection){
            mongoose.connection.close(() => {});
        }
    }
}

module.exports = MongoWrapper;