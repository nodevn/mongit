const _ = require('lodash');
const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const TYPES = require('tedious').TYPES;
const mongoose = require('mongoose');
const dbConfig = require('./database');
const SqlConnection = require('./sqlcon')


const Schema = mongoose.Schema;
const ObjectId = Schema.ObjectId;

class Translation extends SqlConnection {

    constructor(options) {
        super(options);
        this.tableTokens = `ORDINAL_POSITION, COLUMN_NAME, TABLE_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX`.split(/[,\s]/).filter(Boolean);
    }


    getTables() {
        return new Promise((resolve, reject) => {
            let conn = new Connection(this.config);

            conn.on('errorMessage', reject);
            conn.on('connect', err => {
                if (err) {
                    return reject(err);
                } else {
                    let tableInfo = {};
                    let query = `
                        SELECT ORDINAL_POSITION, COLUMN_NAME, TABLE_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, 
                        CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX
                        FROM INFORMATION_SCHEMA.COLUMNS
                    `;
                    let request = new Request(query, (err, rowCount) => {
                        if (err) {
                            return reject(err);
                        }
                    });

                    request.on('row', row => {
                        // console.log(row);
                        let types = require('./types');
                        let tableName = row.find(col => col.metadata.colName == 'TABLE_NAME').value;
                        let fieldName = row.find(col => col.metadata.colName == 'COLUMN_NAME').value;
                        tableInfo[tableName] = tableInfo[tableName] || {};
                        for (let col of row) {
                            let attribute = col.metadata.colName;
                            let schema = tableInfo[tableName][fieldName]  = tableInfo[tableName][fieldName] || {};
                            schema[attribute] = col.value;
                        }
                    });

                    request.on('done', (rowCount) => {
                        console.log('DONEEEEEE !!!!!!!!!!!!!!!');
                        resolve(tableInfo);
                    });

                    request.on('doneProc', (rowCount) => {
                        console.log('DONEEEEEE !!!!!!!!!!!!!!!');
                        resolve(tableInfo);
                    });

                    conn.execSql(request);
                }
            });
        });
    }

}

// Class: Translation
module.exports = Translation;