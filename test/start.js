var path = require('path');

/**
 * Import specs
 */
var dir = '../test/spec/';
[
  'sqlConnTest',
  'sqlQueryTest',
  'transferTableTest',
  'endTest',
].forEach((script) => {
  require(path.join(dir, script));
});