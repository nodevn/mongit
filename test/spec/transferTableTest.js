const test = require('tape');
const dbConfig = require('./dbConfig');
const SqlConnection = require('../../sqlcon');
const mongoose = require('mongoose');

mongoose.Promise = require('bluebird');

test('Transfer MSSQL to Mongodb', function (t) {
    t.plan(1);
    // init new connection
    let sqlCon = new SqlConnection(dbConfig.sql_connection);
    let mdbCon = dbConfig.mdb_connection;
    let mdbConStr = `mongodb://${mdbCon.username}:${mdbCon.password}@${mdbCon.hostname}/${mdbCon.database}`;
    let transferTableTest = dbConfig.sql_connection.transferTableTest;
    console.log('sql_connection:', dbConfig.sql_connection);
    console.log('mongodb_connection:', mdbConStr);
    sqlCon.tableRowsCount(transferTableTest)
        .then(rowsCount => {
            if (rowsCount > 0) {
                return sqlCon.transferTable(transferTableTest, {
                    mongodb: mdbConStr
                })
            } else if (rowsCount === 0) {
                return Promise.reject('No rows to export !');
            } else {
                return Promise.reject('No table found !');
            }
        })
        .then(transferedRecords => {
            // console.log('tableRowsCount:', transferedRecords);
            t.true(transferedRecords.length > 0, 'transferedRecords should be greater than 0');
        })
        .catch(err => {
            t.end(err);
        });
});