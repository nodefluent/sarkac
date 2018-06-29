"use strict";

const debug = require("debug")("sarkac:http");
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const pjson = require("./../package.json");

const DEFAULT_PORT = 8033;

class HttpServer {

    constructor(config, sarkac){
        this.config = config;
        this.sarkac = sarkac;
        this.server = null;
    }

    async start(){

        if(!this.config.http || !this.config.http.enabled){
            debug("HttpServer disabled in config.");
            return false;
        } else {
            debug("HttpServer enabled in config, starting..");
        }

        const app = express();

        app.use(cors());
        app.use(bodyParser.json({extended: false}));

        app.get("/", (req, res) => {
            res.end(`sarkac/${pjson.version}`);
        });

        app.get("/status", async (req, res) => {
            res.json(await this.sarkac.getStats());
        });

        app.get("/dsl", (req, res) => {
            res.json(this.sarkac.dslHandler.dsl);
        });

        app.get("/calculated", (req, res) => {
            res.json(this.sarkac.dslHandler.cache);
        });

        app.get("/topics", (req, res) => {
            res.json(this.sarkac.dslHandler.topics);
        });

        app.delete("/db", async (req, res) => {
            res.json(await this.sarkac.mongo.getModel("sigma").clearCollection());
        });

        this.server = await new Promise((resolve, reject) => {
            let server = null;
            server = app.listen(this.config.port || DEFAULT_PORT, (error) => {

                if(error){
                    return reject(error);
                }

                resolve(server);
            });
        });

        debug("Listening on port", this.config.port || DEFAULT_PORT);
        return true;
    }

    close(){
        debug("Closing..");
        if(this.server){
            this.server.close();
        }
    }
}

module.exports = HttpServer;