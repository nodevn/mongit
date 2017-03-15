// select 42, 'hello world'
const test = require('tape');
const dbConfig = require('./dbConfig');
const SqlConnection = require('../../sqlcon');

test('SQL get query data', function (t) {
    t.plan(1);
    // init new connection
    const sqlCon = new SqlConnection(dbConfig.sql_connection);
    const sqlQuery = `select 42, 'hello mongit'`;
    sqlCon.getSqlData(sqlQuery)
        .then(dataRow => {
            console.log('dataRow:', JSON.stringify(dataRow));
            t.true(dataRow.length > 0, 'Number of sql data is greater than zero');
        })
        .catch(err => {
            t.end(err);
        });
});
