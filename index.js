"use strict";

// @IMPORTS
const Application = require("neat-base").Application;
const Module = require("neat-base").Module;
const Tools = require("neat-base").Tools;
const fs = require("fs");
const path = require("path");
const Promise = require("bluebird");

module.exports = class Projection extends Module {

    static defaultConfig() {
        return {
            dbModuleName: "database",
            exportconfigpath: "config/importexportcsv",
            colSeparator: ";",
            lineSeparator: "\n",
            map: {
                true: "Ja",
                false: "Nein",
                null: ""
            }
        }
    }

    /**
     *
     */
    init() {
        return new Promise((resolve, reject) => {
            this.log.debug("Initializing...");
            resolve(this);
        });
    }

    loadConfig(configName) {
        return require(path.resolve(path.join(Application.config.root_path, this.config.exportconfigpath, configName + ".js")));
    }

    generateDummy(configName, writeLine) {
        return new Promise((resolve, reject) => {
            let row = [];
            let config = this.loadConfig(configName);

            for (let i = 0; i < config.fields.length; i++) {
                row.push([
                    this.getHeaderConfig(config, config.fields[i]),
                ]);
            }

            writeLine(this.getCsvRow(row) + this.config.lineSeparator);
            row = [];
            for (let i = 0; i < config.fields.length; i++) {
                row.push(this.getDummyColFromConfig(config, config.fields[i]))
            }
            writeLine(this.getCsvRow(row) + this.config.lineSeparator);

            resolve();
        });
    }

    getHeaderConfig(config, col) {
        return col.label;
    }

    getCsvRow(data) {
        let result = [];
        for (let i = 0; i < data.length; i++) {
            let val = this.escapeForCsv(data[i]);
            result.push(val);
        }
        return result.join(this.config.colSeparator);
    }

    escapeForCsv(val) {
        if (typeof val === "string") {
            val = '"' + val + '"'
        }

        return val;
    }

    getDummyColFromConfig(config, col) {
        let model;
        if (col.ref) {
            model = Application.modules.database.getModel(col.ref);
        } else {
            model = Application.modules.database.getModel(config.model);
        }

        let paths = model.schema.paths;
        let path = paths[col.path];

        if (path) {
            if (path.options.enum) {
                return path.options.enum.filter(v => !!v).join(",");
            }
        }

        return "";
    }
}