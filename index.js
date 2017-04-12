"use strict";

// @IMPORTS
const Application = require("neat-base").Application;
const Module = require("neat-base").Module;
const Tools = require("neat-base").Tools;
const fs = require("fs");
const path = require("path");
const Promise = require("bluebird");
const readline = require('readline');

module.exports = class Projection extends Module {

    static defaultConfig() {
        return {
            dbModuleName: "database",
            exportconfigpath: "config/importexportcsv",
            colSeparator: ";",
            lineSeparator: "\n",
            booleanMap: {
                "Ja": true,
                "Nein": false
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

    importFile(configName, file) {
        return new Promise((resolve, reject) => {
            let lineReader = readline.createInterface({
                input: fs.createReadStream(path.resolve(path.join(Application.config.root_path, file)))
            });
            let config = this.loadConfig(configName);
            let firstLine = true;
            let lineCount = 0;

            lineReader.on('line', (line) => {
                lineCount++;
                this.log.debug("Line Reader reading line " + lineCount);
                if (firstLine) {
                    firstLine = false;
                    this.log.debug("Line Reader skipping header");
                    return;
                }

                lineReader.pause();
                return this.importCsvLine(config, line).then((doc) => {
                    lineReader.resume();
                });
            });

            lineReader.on('close', () => {
                this.log.debug("Line Reader Closed, finishing");
                return resolve();
            });

            lineReader.on('pause', () => {
                this.log.debug("Line Reader paused");
            });

            lineReader.on('resume', () => {
                this.log.debug("Line Reader resumed");
            });

            lineReader.on('SIGCONT', () => {
                this.log.debug("Line Reader now in background...");
            });

            lineReader.on('SIGINT', () => {
                this.log.debug("Line Reader stopped ctrl-c");
                return reject(new Error("ctrl-c detected"));
            });

            lineReader.on('SIGTSTP', () => {
                this.log.debug("Line Reader now in background ctrl-z");
            });
        });
    }

    importCsvLine(config, line) {
        return new Promise((resolve, reject) => {
            return this.getDocFromCsvLine(config, line).then((doc) => {
                this.log.debug("Parsed CSV Line, saving...");
                return doc.save().then(() => {
                    this.log.debug("Doc saved");
                    return resolve(doc);
                }, reject);
            }, reject);
        });
    }

    getDocFromCsvLine(config, line) {
        let sourceModel = Application.modules[this.config.dbModuleName].getModel(config.model);
        let doc = new sourceModel();
        let lineArr = line.split(this.config.colSeparator);
        let refs = {};

        for (let i = 0; i < lineArr.length; i++) {
            let val = lineArr[i];

            if (typeof val === "string") {
                // cut off " for texts if present
                if (val[0] === '"' && val[val.length - 1] === '"') {
                    val = val.substr(1, val.length - 1);
                }

                val = val.trim();
            }

            lineArr[i] = val;
        }

        return Promise.map(config.fields, (col, i) => {
            return new Promise((resolve, reject) => {

                // if it is false, just ignore it
                if (col.import === false) {
                    return resolve();
                }

                let model = this.getModelForField(config, col);
                let paths = model.schema.paths;
                let path = paths[col.path];
                if (col.ref) {
                    path = paths[this.getRefPathFromCol(col)];
                }
                let val = lineArr[i];

                if (!path) {
                    this.log.debug("No path found for path " + col.path);
                } else {
                    if (path.instance === "Boolean") {
                        for (let mapped in this.config.booleanMap) {
                            let v = this.config.booleanMap[mapped];

                            if (val.toLowerCase() === mapped.toLowerCase()) {
                                val = v;
                                break;
                            }
                        }

                        if (typeof val !== "boolean") {
                            val = null;
                        }
                    } else if (path.instance === "Array") {
                        val = val.split(col.separator || ",");
                        val = val.filter(v => !!v);
                    }
                }

                if (col.ref) {
                    if (!refs[col.refPath]) {
                        refs[col.refPath] = [];
                    }

                    refs[col.refPath].push({
                        path: col.path.substr(col.refPath.length),
                        val: val,
                        col: col
                    });
                } else {
                    if (typeof col.set === "function") {
                        return col.set(doc, val).then(resolve, reject);
                    }

                    doc.set(col.path, val);
                }

                resolve();
            });
        }).then(() => {
            if (Object.keys(refs).length) {
                return Promise.map(Object.keys(refs), (refPath) => {
                    let subDoc = null;
                    let duplicateQuery = null;
                    let model = null;

                    for (let i = 0; i < refs[refPath].length; i++) {
                        let refConfig = refs[refPath][i];
                        let realSubPath = refConfig.path.substr(1); // substr 1 because of the .
                        if (!subDoc) {
                            model = Application.modules[this.config.dbModuleName].getModel(refConfig.col.ref);
                            subDoc = new model();
                        }

                        subDoc.set(realSubPath, refConfig.val);

                        if (refConfig.col.isRefIdentifier) {
                            if (!duplicateQuery) {
                                duplicateQuery = {};
                            }
                            duplicateQuery[realSubPath] = subDoc.get(realSubPath);
                        }
                    }

                    if (duplicateQuery) {
                        return model.findOne(duplicateQuery).then((existingSubDoc) => {
                            if (existingSubDoc) {
                                this.log.debug("Found existing subdoc " + existingSubDoc._id);
                                doc.set(refPath, existingSubDoc._id);
                            } else {
                                this.log.debug("Creating new subdoc");
                                return subDoc.save().then(() => {
                                    doc.set(refPath, subDoc._id);
                                });
                            }
                        });
                    } else {
                        this.log.debug("Creating new subdoc");
                        return subDoc.save().then(() => {
                            doc.set(refPath, subDoc._id);
                        });
                    }

                }).then(() => {
                    return doc;
                });
            }

            return doc;
        });
    }

    getRefPathFromCol(col) {
        return col.path.substr(col.refPath.length + 1); // add 1 because of the . in the path
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

    getModelForField(config, col) {
        if (col.ref) {
            return Application.modules[this.config.dbModuleName].getModel(col.ref);
        } else {
            return Application.modules[this.config.dbModuleName].getModel(config.model);
        }
    }

    getDummyColFromConfig(config, col) {
        let model = this.getModelForField(config, col);
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