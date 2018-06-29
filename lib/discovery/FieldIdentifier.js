"use strict";

class FieldIdentifier {

    constructor(config = {}){
        this.config = config;
    }

    async analyseMessageSchema(message) {

        const bag = [];

        if(!message){
            return bag;
        }

        if(typeof message.value === "object"){

            if(Buffer.isBuffer(message.value)){
                return bag;
            }

            this.marshallObject(message.value, "", bag);
        } else {
            bag.push({
                path: "",
                type: typeof message.value
            });
        }

        //TODO: can we handle other types as well?
        return bag
            .filter((field) => field.type === "number")
            .map((field) => field.path);
    }

    marshallObject(object, parentPath, bag){
        Object.keys(object).forEach((key) => {

            const val = object[key];
            const keyPath = !!parentPath ? parentPath + "." + key : key;

            if(typeof val === "object"){

                if(Buffer.isBuffer(val)){
                    return;
                }

                this.marshallObject(val, keyPath, bag);
            } else {
                bag.push({
                    path: keyPath,
                    type: typeof val
                });
            }
        });
    }
}

module.exports = FieldIdentifier;