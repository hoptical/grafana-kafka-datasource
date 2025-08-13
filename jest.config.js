// force timezone to UTC to allow tests to work regardless of local timezone
// generally used by snapshots, but can affect specific tests
process.env.TZ = 'UTC';

const baseConfig = require('./.config/jest.config');

module.exports = {
  // Jest configuration provided by Grafana scaffolding
  ...baseConfig,
  // Add react-calendar and its dependencies to modules that should be transformed
  transformIgnorePatterns: [
    'node_modules/(?!.*(@grafana|d3|ol|rxjs|uuid|marked|react-colorful|react-calendar|get-user-locale|memoize).*)'
  ],
};
