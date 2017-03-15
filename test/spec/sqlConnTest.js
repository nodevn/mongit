const test = require('tape');
const dbConfig = require('./dbConfig');
const SqlConnection = require('../../sqlcon');

test('SQL connection ready', function (t) {
    t.plan(1);
    // init new connection
    const sqlCon = new SqlConnection(dbConfig.sql_connection);
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
    let sqlCon = new SqlConnection(dbConfig.sql_connection);
    sqlCon.tableRowsCount('sysrowsets', true)
        .then(rowsCount => {
            t.notEqual(rowsCount, 0, 'rows of sys.objects should be greater than 0');
        }).catch(err => {
            t.end(err);
        });
});