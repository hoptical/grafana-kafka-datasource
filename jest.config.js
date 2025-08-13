// force timezone to UTC to allow tests to work regardless of local timezone
// generally used by snapshots, but can affect specific tests
process.env.TZ = 'UTC';

const baseConfig = require('./.config/jest.config');
const { grafanaESModules, nodeModulesToTransform } = require('./.config/jest/utils');
module.exports = {
  // Jest configuration provided by Grafana scaffolding
  ...baseConfig,
  // Add react-calendar and its dependencies to modules that should be transformed
  transformIgnorePatterns: [
    nodeModulesToTransform([...grafanaESModules, 'react-calendar', 'get-user-locale', 'memoize']),
  ],
};
