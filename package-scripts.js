module.exports = {
  scripts: {
    copy: {
      css: "rsync -ar src/css/vendor/ application/static/stylesheets/vendor",
      js: "rsync -ar src/javascripts/vendor/ application/static/javascripts/vendor"
    },
    build: {
      stylesheets: "sass src/scss:application/static/stylesheets " +
        "--load-path=node_modules/govuk-frontend/dist " +
        "--load-path=node_modules/digital-land-frontend " +
        "--load-path=src/scss/components " +
        "--load-path=src/scss/common",
      javascripts: "esbuild src/javascripts/main.js --bundle --outfile=application/static/javascripts/bundle.js"
    }
  }
};
