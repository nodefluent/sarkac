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
            res.json({

                "Info": `sarkac/${pjson.version}`,

                "Self": "GET /",
                "Status": "GET /status",
                "Healthcheck": "GET /healthcheck",

                "Loaded DSL": "GET /dsl",
                "Computed DSL": "GET /dsl/computed",
                "DSL Topics": "GET /dsl/topics",

                "Discovered Topics": "GET /discovery/topics",
                "Discovered Fields": "GET /discovery/fields",
                "Discovered Hashes": "GET /discovery/hashes",

                "Truncate Database": "DELETE /db/truncate",
            });
        });

        app.get("/status", async (req, res) => {
            res.json(await this.sarkac.getStats());
        });

        app.get("/healthcheck", async (req, res) => {
            res.status(200).end();
        });

        app.get("/dsl", (req, res) => {
            res.json(this.sarkac.dslHandler.dsl);
        });

        app.get("/dsl/computed", (req, res) => {
            res.json(this.sarkac.dslHandler.cache);
        });

        app.get("/dsl/topics", (req, res) => {
            res.json(this.sarkac.dslHandler.topics);
        });

        app.get("/discovery/topics", (req, res) => {
            res.json(this.sarkac.discovery.getDiscoveredTopics());
        });

        app.get("/discovery/fields", (req, res) => {
            res.json(this.sarkac.discovery.getDiscoveredFields());
        });

        app.get("/discovery/hashes", (req, res) => {
            res.json(this.sarkac.discovery.getHashes());
        });

        app.delete("/db/truncate", async (req, res) => {
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