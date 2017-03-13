const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const TYPES = require('tedious').TYPES;
const mongoose = require('mongoose');
const dbConfig = require('./database');


const Schema = mongoose.Schema;
const ObjectId = Schema.ObjectId;

class SqlConnection {

    constructor(options) {
        // setup default config
        this.config = {};

        // override default config
        for (let opt in options) {
            if (options.hasOwnProperty(opt)) {
                this.config[opt] = options[opt];
            }
        }
    }

    testServer() {
        return new Promise((resolve, reject) => {
            let conn = new Connection(this.config);

            conn.on('errorMessage', reject);
            conn.on('connect', err => {
                if (err) {
                    reject(err);
                } else {
                    let dbList = [];
                    let request = new Request('sp_databases', err => {
                        if (err) {
                            reject(err);
                        }
                    });

                    request.on('row', row => {
                        dbList.push(row[0].value);
                    });

                    request.on('doneProc', (rowCount, more) => {
                        resolve(dbList);
                    });

                    conn.callProcedure(request);
                }
            });
        });
    }

    tableRowsCount(table) {
        return new Promise((resolve, reject) => {
            let conn = new Connection(this.config);

            conn.on('errorMessage', reject);
            conn.on('connect', err => {
                if (err) {
                    return reject(err);
                } else {
                    let query = `
                        SELECT SUM(rows) AS Rows
                        FROM sys.partitions p
                        LEFT JOIN sys.objects obj ON obj.object_id = p.object_id
                        WHERE (p.index_id in (0, 1)) 
                        AND (obj.is_ms_shipped = 0)
                        AND (obj.type_desc = 'USER_TABLE')
                        AND (obj.name = '${table}')
                    `;
                    let request = new Request(query, (err, rowCount) => {
                        if (err) {
                            return reject(err);
                        } else {
                            console.log('rowCount: ', rowCount);
                        }
                    });

                    request.on('row', result => {
                        resolve(result[0].value);
                    });

                    conn.execSql(request);
                }
            });
        });
    }

    transferTable(table, options) {
        return new Promise((resolve, reject) => {
            let rows = [];
            let columns = [];
            let schema = {};
            let ItemSchema = {};
            let ItemModel = {};
            let request = {};
            let mongoDbServer = options.mongodb;
            let lastRowId = 0;
            let count = 10000;
            let insertRows = 0;
            let readRows = 0;
            let types = require('./types');
            let dbConfig = this.config;


            this.tableRowsCount(table)
                .then(tableRowsCount => {
                    console.log('tableRowsCount: ', tableRowsCount);
                    // start transfer
                    mongoose.connect(mongoDbServer);
                    startTransfer(lastRowId, count, tableRowsCount, (err, notifyMsg) => {
                        if (err) {
                            console.error('startTransfer ERROR:', err);
                            reject(err);
                        } else {
                            if (notifyMsg === 'Transfer Finished!') {
                                resolve(tableRowsCount);
                            }
                        }
                    });
                })
                .catch(err => {
                    reject(err);
                });

            function startTransfer(start, count, tableRowsCount, callback) {
                let iStart = parseInt(start);
                if (iStart >= tableRowsCount) {
                    return callback(null, 'Transfer Finished!');
                }
                let query = `
                    DECLARE @columnName NVARCHAR(60)
                    SET @columnName = (SELECT TOP 1 NAME FROM sys.columns WHERE object_id = OBJECT_ID('dbo.${table}'))
                    SELECT * FROM (
                        SELECT ROW_NUMBER() OVER(ORDER BY @columnName) AS row, T.*
                        FROM (SELECT * FROM ${table}) T
                    ) T2 WHERE T2.row BETWEEN ${start} AND ${start + count}
                    `;
                let sqlcon = new Connection(dbConfig);
                sqlcon.on('errorMessage', err => {
                    callback(err);
                });
                sqlcon.on('connect', err => {
                    if (err) {
                        console.log('$startTransfer:', err);
                        callback(err);
                    } else {
                        let request = new Request(query, err => {
                            if (err) {
                                return callback(err);
                            }
                        });

                        request.on('columnMetadata', allColumns => {
                            if (insertRows === 0) {
                                // reset schema fields.
                                schema = {};
                                for (let column of allColumns) {
                                    let type = types[column.type.name];
                                    schema[column.colName] = type;
                                    console.info(`Cast type [${column.type.name} to ${type}]`);
                                }
                                ItemSchema = new Schema(schema);
                                try {
                                    ItemModel = mongoose.model(table);
                                } catch (err) {
                                    ItemModel = mongoose.model(table, ItemSchema);
                                }
                            }
                        });

                        request.on('row', row => {
                            if (++readRows % 1000 == 0) {
                                callback(null, readRows);
                            }

                            let newRow = {};
                            for (let col of row) {
                                for (let field in schema) {
                                    if (field === col.metadata.colName) {
                                        newRow[field] = col.value;
                                        break;
                                    }
                                }
                            }
                            console.log('readRows: ', readRows);
                            let newItem = new ItemModel(newRow);
                            newItem.save((err, result) => {
                                if (err) {
                                    console.error('ERROR save failed: ', err);
                                    callback(err.stack || err);
                                } else {
                                    console.log('insertRows: ', insertRows);
                                    if ((++insertRows % 1000) === 0) {
                                        callback(null, insertRows);
                                    }
                                }
                            });
                        });

                        request.on('doneProc', () => {
                            lastRowId = lastRowId + count;
                            startTransfer(lastRowId, count, tableRowsCount, callback);
                        });

                        // start sql
                        sqlcon.execSql(request);
                    }
                });
            }
        });
    }
}

// Class: SqlConnection
module.exports = SqlConnection;