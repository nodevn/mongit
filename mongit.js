const dbConfig = require('./database').session;
const MongoDb = require('mongodb');


const MongodbServer = new MongoDb.Server(dbConfig.host, dbConfig.port, dbConfig.serverOptions);
const MongoDbConnection = new MongoDb.Db(dbConfig.dbName, MongodbServer, dbConfig.dbOptions);
// const mongoStore = require('connect-mongodb');
