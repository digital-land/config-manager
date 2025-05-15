module.exports = {
  scripts: {
    build: {
      stylesheets: "sass src/scss:application/static/stylesheets " +
        "--load-path=node_modules/govuk-frontend/dist " +
        "--load-path=node_modules/digital-land-frontend " +
        "--load-path=src/scss/components " +
        "--load-path=src/scss/common"
    }
  }
};
