"use strict";

// @IMPORTS
const Application = require("neat-base").Application;
const Module = require("neat-base").Module;
const Tools = require("neat-base").Tools;
const fs = require("fs");
const path = require("path");
const Promise = require("bluebird");
const os = require("os");
const readline = require('readline');
const archiver = require('archiver');

module.exports = class Projection extends Module {

    static defaultConfig() {
        return {
            dbModuleName: "database",
            exportconfigpath: "config/importexport",
            colSeparator: ";",
            lineSeparator: "\r\n",
            archiver: {
                method: "zip",
                options: {
                    store: true
                }
            },
            export: {
                perPage: 10,
                concurrency: 10
            },
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
            config.duplicates = [];
            config.errors = [];
            let firstLine = true;
            let lineCount = 0;
            let inProgress = 0;

            lineReader.on('line', (line) => {
                lineCount++;
                this.log.debug("Line Reader reading line " + lineCount);
                if (firstLine) {
                    firstLine = false;
                    this.log.debug("Line Reader skipping header");
                    return;
                }

                inProgress++;
                lineReader.pause();
                return this.importCsvLine(config, line, lineCount).then((doc) => {
                    inProgress--;
                    // only resume if all others are done
                    this.log.debug("Saved, check if others are done too, " + inProgress + " transactions remaining");
                    if (inProgress === 0) {
                        lineReader.resume();
                    }
                }, (err) => {
                    lineReader.close();
                    return reject(err);
                });
            });

            lineReader.on('close', () => {
                this.log.debug("Line Reader Closed, finishing");
                setInterval(() => {
                    this.log.debug("Checking if done, " + inProgress + " transactions remaining");
                    if (inProgress === 0) {
                        this.log.info("Found " + config.duplicates.length + " Duplicates here are the line numbers:");
                        this.log.info(config.duplicates.join(","));
                        this.log.info("Found " + config.errors.length + " Errors here are the line numbers and the messages:");
                        for (let i = 0; i < config.errors.length; i++) {
                            let errObj = config.errors[i];
                            this.log.info("Line: " + errObj.lineNumber + " Message: " + errObj.err.toString());

                        }
                        return resolve();
                    }
                }, 300)
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

    importCsvLine(config, line, lineNumber) {
        let doc;
        return new Promise((resolve, reject) => {
            return this.getDocFromCsvLine(config, line).then((docFromCsv) => {
                doc = docFromCsv;
                if (config.isDuplicate) {
                    return config.isDuplicate(doc);
                } else {
                    return Promise.resolve(true);
                }
            }, reject).then((isDuplicate) => {
                if (isDuplicate) {
                    this.log.debug("Duplicate found " + lineNumber);
                    process.stdout.write("D");
                    config.duplicates.push(lineNumber);
                    return resolve();
                }

                if (this.config.debug) {
                    this.log.debug(doc.toJSON());
                    return resolve(doc);
                }

                this.log.debug("Parsed CSV Line, saving...");
                return doc.save(config.saveOptions).then(() => {
                    process.stdout.write("S");
                    this.log.debug("Doc saved");
                    return resolve(doc);
                }, (err) => {
                    process.stdout.write("!");
                    config.errors.push({
                        err: err,
                        lineNumber: lineNumber,
                        doc: doc
                    });
                    return resolve();
                });
            }, (err) => {
                process.stdout.write("!");
                config.errors.push({
                    err: err,
                    lineNumber: lineNumber,
                    doc: doc
                });
                return resolve();
            });
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
                val = val.trim();

                // cut off " for texts if present
                if (val[0] === '"' && val[val.length - 1] === '"') {
                    val = val.substr(1, val.length - 2);
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
                if (val === "") {
                    val = undefined;
                }

                if (!path) {
                    this.log.debug("No path found for path " + col.path);
                } else {
                    if (path.instance === "Boolean") {
                        if (typeof val === "string") {
                            for (let mapped in this.config.booleanMap) {
                                let v = this.config.booleanMap[mapped];

                                if (val.toLowerCase() === mapped.toLowerCase()) {
                                    val = v;
                                    break;
                                }
                            }
                        }

                        if (typeof val !== "boolean") {
                            val = null;
                        }
                    } else if (path.instance === "Array") {
                        if (typeof val === "string") {
                            val = val.split(col.separator || ",");
                            val = val.filter(v => !!v);
                        }
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

                    if (!col.path && !val) {
                        return resolve();
                    }

                    doc.set(col.path, val);
                }

                if (col.required && !val) {
                    return reject(new Error("Column " + col.label + " with path " + col.path + " is required but was empty!"));
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
                                return subDoc.save(config.saveOptions).then(() => {
                                    doc.set(refPath, subDoc._id);
                                });
                            }
                        });
                    } else {
                        this.log.debug("Creating new subdoc");
                        return subDoc.save(config.saveOptions).then(() => {
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

    export(configName, query, outFile, infoFunc) {
        infoFunc = infoFunc || function (info) {

        };
        let info = null;
        let self = this;
        return this.setupExport(configName, query, outFile).then((tmpInfo) => {
            info = tmpInfo;
            infoFunc(info);
            this.log.debug("Starting export..");
            return this.exportAll(tmpInfo);
        }).then(() => {
            return new Promise((resolve, reject) => {
                this.log.debug("Finalizing archive");
                info.archive.on('error', function (err) {
                    return reject(err);
                });

                info.archive.file(info.csv, {name: "export.csv"});
                info.archive.finalize();
                return resolve(info);
            });
        });
    }

    setupExport(configName, query, outFile) {
        return new Promise((resolve, reject) => {
            let folderName = "Export_" + configName + "_" + (new Date().getTime());
            let tmpDir = path.join(os.tmpdir(), folderName);
            let csvPath = path.join(tmpDir, "export.csv");
            let imgDir = path.join(tmpDir, "images");
            let config = this.loadConfig(configName);
            let archive = archiver(this.config.archiver.method, this.config.archiver.options);
            let info = {
                archive: archive,
                tmpDir: tmpDir,
                folderName: folderName,
                config: config,
                knownFiles: [],
                query: query,
                outFile: outFile,
                model: Application.modules[this.config.dbModuleName].getModel(config.model),
                csv: csvPath,
                imgDir: imgDir,
                currentPage: 0,
                lastLineNumber: 1 // one because of the header
            };

            fs.mkdirSync(tmpDir);
            fs.mkdirSync(imgDir);

            let headerFields = config.fields.map(v => v.label);
            fs.appendFileSync(info.csv, this.getCsvRow(headerFields) + this.config.lineSeparator);

            return info.model.count(info.query).then((count) => {
                info.totalCount = count;
                info.totalPages = Math.ceil(count / this.config.export.perPage);
                return resolve(info);
            });
        });
    }

    exportAll(info) {
        return new Promise((resolve, reject) => {
            if (info.currentPage > info.totalPages) {
                return resolve();
            }

            return this.exportPage(info)
                .then(() => {
                    this.log.debug("Exported Page", info.currentPage);
                    info.currentPage++;
                    return this.exportAll(info);
                }, reject)
                .then(resolve, reject)
        });
    }

    exportPage(info) {
        return info.model
            .find(info.query)
            .skip(info.currentPage * this.config.export.perPage)
            .limit(this.config.export.perPage)
            .sort({
                _id: 1
            })
            .populate(info.config.populate || [])
            .then((docs) => {
                return Promise.map(docs, (doc) => {
                    return this.exportItem(doc, info);
                }, {
                    concurrency: this.config.export.concurrency
                });
            })
    }

    exportItem(doc, info) {
        return this.getExportDataFromDoc(doc, info.config).then((data) => {
            info.lastLineNumber++;
            this.log.debug("Appending row " + info.lastLineNumber + " to csv");

            let line = this.getCsvRow(data.data);

            if (!line) {
                this.log.warn("Found empty csv line " + doc._id);
            }

            fs.appendFileSync(info.csv, line + this.config.lineSeparator);

            if (!data.files || !Object.keys(data.files).length) {
                return;
            }

            for (let fileName in data.files) {
                let filePath = data.files[fileName];
                filePath = path.join(Application.config.root_path, filePath);

                try {
                    if (!fs.statSync(filePath).isFile()) {
                        throw new Error("catch me if you can!");
                    }
                } catch (e) {
                    this.log.warn("File " + filePath + " is not a file, skipping");
                    continue;
                }


                if (info.knownFiles.indexOf(fileName) === -1) {
                    info.archive.file(filePath, {name: "images/" + fileName});
                    info.knownFiles.push(fileName);
                }
            }
        });
    }

    getExportDataFromDoc(doc, config) {
        let result = {
            data: null,
            files: []
        };

        return Promise.map(config.fields, (col, i) => {
            // if it is false, just ignore it
            if (col.export === false) {
                return resolve();
            }

            let model = this.getModelForField(config, col);
            let paths = model.schema.paths;
            let path = paths[col.path];
            let exportValPromise = Promise.resolve("");

            if (col.ref) {
                path = paths[this.getRefPathFromCol(col)];
            }

            if (col.get) {
                return col.get(doc);
            }

            if (path) {
                if (path.instance === "Boolean") {
                    let val = doc.get(col.path);

                    for (let label in this.config.booleanMap) {
                        if (val === this.config.booleanMap[label]) {
                            exportValPromise = Promise.resolve(label)
                        }
                    }
                } else if (path.instance === "Array") {
                    let val = doc.get(col.path);

                    if (!val) {
                        val = [];
                    }

                    exportValPromise = Promise.resolve(val.join(col.separator));
                } else {
                    exportValPromise = Promise.resolve(doc.get(col.path));
                }
            } else if (col.path) {
                exportValPromise = Promise.resolve(doc.get(col.path));
            }

            return exportValPromise;
        }).then((data) => {
            result.data = data;
            if (!config.files) {
                // no need to get the connected files in this case, just resolve an empty object
                return Promise.resolve({});
            }

            return doc.getConnectedFiles();
        }).then((connectedFiles) => {
            if (!config.files) {
                return result;
            }

            return config.files(doc, connectedFiles);
        }).then((files) => {
            result.files = files;
            return result;
        });
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
            // escape double quotes and remove line breaks.
            val = val.replace(/"/g, '""').replace(/\s/g, ' ');
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