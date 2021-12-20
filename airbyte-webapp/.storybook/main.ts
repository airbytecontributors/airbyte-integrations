const CircularDependencyPlugin = require('circular-dependency-plugin')

module.exports = {
  stories: ["../src/**/*.stories.@(ts|tsx)"],
  addons: [
    // "@storybook/addon-links",
    "@storybook/addon-essentials",
    "@storybook/preset-create-react-app",
    "storybook-addon-styled-component-theme/dist",
  ],
  webpackFinal: async (config) => {
    config.plugins.push(new CircularDependencyPlugin())

    return config;
  },
  // webpackFinal: (config) => {
  //   config.resolve.modules.push(process.cwd() + "/node_modules");
  //   config.resolve.modules.push(process.cwd() + "/src");
  //
  //   // this is needed for working w/ linked folders
  //   config.resolve.symlinks = false;
  //   return config;
  // },
  core: {
    builder: "webpack5",
  },
};
