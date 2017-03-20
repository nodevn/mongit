const test = require('tape');
const dbConfig = require('./dbConfig');
const Translation = require('../../translation');
const mongoose = require('mongoose');

mongoose.Promise = require('bluebird');

test('Translation SQL Table to config files', function (t) {
    t.plan(1);
    // init new connection
    let trans = new Translation(dbConfig.sql_connection);
    trans.getTables()
        .then(tables => {
            // console.log(tables);
            require('fs').writeFileSync('./data.json', JSON.stringify(tables, null, 2), 'utf-8'); 
            t.true(tables);
            t.end();
        })
        .catch(err => {
            t.end(err);
        });
});