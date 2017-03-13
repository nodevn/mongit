const test = require('tape');
const dbConfig = require('./dbConfig');
const SqlConnection = require('../../sqlcon');

test('SQL connection ready', function (t) {
    t.plan(1);
    // init new connection
    const sqlCon = new SqlConnection(dbConfig);
    sqlCon.testServer()
        .then(dbList => {
            console.log('dbList:', dbList);
            t.true(dbList.length > 0, 'Number of databases is greater than zero');
        })
        .catch(err => {
            t.end(err);
        });
});

test('Count table rows number from master db', (t) => {
    t.plan(1);
    let sqlCon = new SqlConnection(dbConfig);
    sqlCon.tableRowsCount('spt_values')
        .then(rowsCount => {
            t.true(rowsCount > 0, 'rowsCount should be greater than zero');
        }).catch(err => {
            t.end(err);
        });
});