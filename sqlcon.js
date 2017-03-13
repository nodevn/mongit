const Connection = require('tedious').Connection;
const Request = require('tedious').Request;
const TYPES = require('tedious').TYPES;
const dbConfig = require('./database');

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

    tableCount(table) {
        return new Promise((resolve, reject) => {
            let conn = new Connection(this.config);

            conn.on('errorMessage', reject);
            conn.op('connect', err => {
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
                        AND (obj.name = ${table})
                    `;
                    let request = new Request(query, (err, rowCount) => {
                        if (err) {
                            return reject(err);
                        } else {
                            console.log('rowCount: ', rowCount);
                        }
                    });

                    request.on('row', result => {
                        console.log('onRow:', result);
                        resolve(result[0].value);
                    });

                    conn.execSql(request);
                }
            });
        });
    }

    getTable() {

    }
}

// Class: SqlConnection
module.exports = SqlConnection;