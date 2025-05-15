module.exports = {
  scripts: {
    "copy.javascripts": "rsync -ar src/javascripts/vendor/ application/static/javascripts/vendor",
    "copy.css": "rsync -ar src/css/vendor/ application/static/stylesheets/vendor",
    copy: "npm-run-all --parallel copy.*",

    "build.stylesheets": "sass src/scss:application/static/stylesheets " +
      "--load-path=node_modules/govuk-frontend/dist " +
      "--load-path=node_modules/digital-land-frontend " +
      "--load-path=src/scss/components " +
      "--load-path=src/scss/common",

    "build.javascripts": "esbuild src/javascripts/main.js --bundle --outfile=application/static/javascripts/bundle.js"
  }
};
