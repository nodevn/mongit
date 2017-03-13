var path = require('path');

/**
 * Import specs
 */
var dir = '../test/spec/';
[
  'sqlconnTest',
  'transferTableTest',
  'endTest',
].forEach((script) => {
  require(path.join(dir, script));
});