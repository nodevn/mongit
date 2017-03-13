const test = require('tape');
const SqlConnection = require('../../sqlcon');

test('SQL connection ready', function (t) {
    t.plan(1);
    // init new connection
    const sqlCon = new SqlConnection({
        userName: 'sa',
        password: 'sa123456',
        server: 'localhost',
        options: {
            port: 49175,
            database: 'master',
            instancename: 'SQLEXPRESS'
        }
    });
    sqlCon.testServer()
        .then(dbList => {
            console.log('dbList:', dbList);
            t.true(dbList.length > 0, 'Number of databases is greater than zero');
        })
        .catch(err => {
            t.fail(err);
        });
});