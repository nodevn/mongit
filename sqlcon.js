const _ = require('lodash');
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
        this.Schema = Schema;
        this.ObjectId = ObjectId;

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

    /**
     * Count rows number of the table
     * 
     * @param {String} table wants to count
     * @param {Boolean} is_ms_shipped indicates this object was shipped or created by Microsoft
     */
    tableRowsCount(table, is_ms_shipped) {
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
                        AND (obj.is_ms_shipped = ${!is_ms_shipped ? 0 : 1})
                        AND (obj.type_desc = '${!is_ms_shipped ? 'USER_TABLE' : 'SYSTEM_TABLE'}')
                        AND (obj.name = '${table}')
                    `;
                    let request = new Request(query, (err, rowCount) => {
                        if (err) {
                            return reject(err);
                            // } else {
                            //     console.log('rowCount: ', rowCount, query);
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
            let lastRowId = 0;
            let count = 10000;
            let insertRows = 0;
            let readRows = 0;
            let dbConfig = this.config;
            // let mongoConnection = mongoose.createConnection(options.mongodb);
            let mongoConnection = mongoose.connect(options.mongodb, (err) => {
                if (err) {
                    return reject(err);
                }
                // preparing data
                this.tableRowsCount(table)
                    .then(tableRowsCount => {
                        console.log('tableRowsCount: ', tableRowsCount);
                        // start transfer

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
                // end: connect mongoose
            });

            mongoose.connection.on('connected', () => {
                console.log('[+] MongoDB connection established!');
            });
            mongoose.connection.on('error', () => {
                console.log('[-] MongoDB connection error. Please make sure MongoDB is running.');
            });

            function startTransfer(start, count, tableRowsCount, callback) {
                let task = new Promise((resolve, reject) => {

                    let iStart = parseInt(start);
                    if (iStart >= tableRowsCount) {
                        return resolve({
                            message: 'Transfer Finished!',
                            tableRowsCount: iStart
                        });
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
                    // console.log('start Query: ', query);
                    sqlcon.on('errorMessage', err => {
                        callback(err);
                    });
                    sqlcon.on('connect', err => {
                        if (err) {
                            console.error('$startTransfer:', err);
                            callback(err);
                        } else {
                            let records = [];
                            let schema = {};
                            let request = new Request(query, err => {
                                if (err) {
                                    return callback(err);
                                }
                            });

                            request.on('columnMetadata', allColumns => {
                                // console.info('columnMetadata', allColumns);
                                if (insertRows === 0) {
                                    // declare schema fields.
                                    let types = require('./types');
                                    for (let column of allColumns) {
                                        let type = types[column.type.name];
                                        schema[column.colName] = type;
                                        console.info(`Cast type [${column.type.name} to ${type}]`);
                                    }
                                    // setup new mongodb schema
                                    let docSchema = new Schema(schema);
                                    mongoose.model(table, docSchema);
                                }
                            });

                            request.on('row', row => {
                                if (++readRows % 1000 == 0) {
                                    callback(null, readRows);
                                }
                                // console.log(row);
                                let newRow = {};
                                for (let col of row) {
                                    for (let field in schema) {
                                        if (field === col.metadata.colName) {
                                            newRow[field] = col.value;
                                            break;
                                        }
                                    }
                                }
                                records.push(newRow);
                            });

                            request.on('doneProc', (rowCount, more, status, rows) => {
                                console.log('doneProc !!!!!!!!!!, readRows:', readRows);
                                lastRowId = lastRowId + count;
                                resolve(records);
                            });

                            // start sql
                            sqlcon.execSql(request);
                        }
                    });
                });
                // end: Promise
                return task.then(dataRows => {
                        return new Promise((resolve, reject) => {
                            mongoose.model(table).create(dataRows)
                                .then(result => {
                                    resolve(result)
                                })
                                .catch(err => {
                                    console.error('ERROR save failed: ', err);
                                    reject(err);
                                });
                        })
                    })
                    .then(result => {
                        // continue ?
                        // startTransfer(lastRowId, count, tableRowsCount, callback);
                        resolve(result);
                    })
                    .catch(err => {
                        callback(err);
                    })
            }
        });
    }

    getSqlData(query) {
        return new Promise((resolve, reject) => {

            let dbConfig = _.extend({}, this.config);
            dbConfig.options['rowCollectionOnRequestCompletion'] = true;
            
            let sqlcon = new Connection(dbConfig);

            sqlcon.on('errorMessage', err => {
                return reject(err);
            });

            sqlcon.on('connect', err => {
                if (err) {
                    return reject(err);
                }
                // start query data;
                console.log('start Query: ', query);
                let request = new Request(query, (err, rowCount, rows) => {
                    if (err) {
                        return reject(err);
                    } else {
                        return resolve(rows);
                    }
                });
               
                // start sql
                sqlcon.execSql(request);
            });
        }) // End: Promise receive data                    
    }
}

// Class: SqlConnection
module.exports = SqlConnection;