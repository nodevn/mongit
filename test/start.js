var path = require('path');

/**
 * Import specs
 */
var dir = '../test/spec/';
[
  'sqlconnTest',
  'endTest',
].forEach((script) => {
  require(path.join(dir, script));
});