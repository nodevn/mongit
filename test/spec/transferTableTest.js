const test = require('tape');
const dbConfig = require('./dbConfig');
const SqlConnection = require('../../sqlcon');
const mongoose = require('mongoose');

mongoose.Promise = require('bluebird');
mongoose.connection.on('connected', () => {
  console.log('%s MongoDB connection established!');
});
mongoose.connection.on('error', () => {
  console.log('%s MongoDB connection error. Please make sure MongoDB is running.');
  process.exit();
});

test('Transfer MSSQL to Mongodb', function (t) {
    t.plan(1);
    // init new connection
    let sqlCon = new SqlConnection(dbConfig.sql_connection);
    let mdbCon = dbConfig.mdb_connection;
    let mdbConStr = `mongodb://${mdbCon.username}:${mdbCon.password}@${mdbCon.hostname}/${mdbCon.database}`;
    sqlCon.transferTable('HT_USER_GROUP', {
        mongodb: mdbConStr
    })
        .then(tableRowsCount => {
            console.log('tableRowsCount:', tableRowsCount);
            t.true(tableRowsCount > 0, 'tableRowsCount');
        })
        .catch(err => {
            t.end(err);
        });
});