"use strict";

const debug = require("debug")("sarkac:model:sigma");
const murmur = require("murmurhash");

class SigmaModel {

    constructor(){
        this.name = "sigma";
        this.model = null;
    }

    registerModel(mongoose, Schema){

        const sigmaSchemaDefinition = {
            key: "number",
            value: "number",
            produced: "number"
        };

        const sigmaSchema = new Schema(sigmaSchemaDefinition);
        this.model = mongoose.model(this.name, sigmaSchema);
        debug("Registered.");
    }

    _getKey(topic, field){
        return murmur.v3(`${topic}:${field}`);
    }

    storeValueOfEvent(topic, field, value, timestamp){
        const key = this._getKey(topic, field);
        return this.model.create({
            key,
            value,
            produced: timestamp
        });
    }

    cleanValuesOfEventForRetention(topic, field, retentionSeconds){
        const key = this._getKey(topic, field);
        return this.model.remove({
            key,
            produced: { $lte: Date.now() - retentionSeconds * 1000}
        });
    }

    getMedianForEventWindow(topic, field, windowSeconds){
        // no median supported, average used instead
        const key = this._getKey(topic, field);
        return this.model.aggregate([
            {
                $match: {
                    key,
                    produced: { $gte: Date.now() - windowSeconds * 1000 }
                }
            },
            {
                "$group": {
                    _id: "$key",
                    median: {
                        "$avg": "$value"
                    }
                }
            }
        ]).then((result) => {

            if(!result || !result.length){
                return 0;
            }

            return result[0].median;
        });
    }

    getStdDevForEventWindow(topic, field, windowSeconds){
        const key = this._getKey(topic, field);
        return this.model.aggregate([
            {
                $match: {
                    key,
                    produced: { $gte: Date.now() - windowSeconds * 1000 }
                }
            },
            {
                "$group": {
                    _id: "$key",
                    stdDev: {
                        "$stdDevPop": "$value"
                    }
                }
            }
        ]).then((result) => {

            if(!result || !result.length){
                return 0;
            }

            return result[0].stdDev;
        });
    }
}

module.exports = SigmaModel;