// Import the base scripts from digital-land-frontend
const dlFrontendScripts = require('@planning-data/digital-land-frontend/package-scripts.js');

// Add your own custom scripts
const customScripts = {
  copy: {
    ...dlFrontendScripts.scripts.copy, // Keep the digital-land-frontend copy scripts
    // Override govukAssets to put files in the correct location for SCSS this is an error with. digital-land-frontend that needs fixing
    // SCSS expects /static/govuk/assets/fonts/, so we use -u 4 to strip node_modules/govuk-frontend/dist/govuk
    govukAssets: 'npx copyfiles -u 4 "node_modules/govuk-frontend/dist/govuk/assets/**" application/static/govuk/',
    vendor: 'mkdir -p application/static/javascripts/vendor application/static/stylesheets/vendor && cp -r src/javascripts/vendor/* application/static/javascripts/vendor/ && cp -r src/css/vendor/* application/static/stylesheets/vendor/',
    accessibleAutocomplete: 'cp node_modules/accessible-autocomplete/dist/accessible-autocomplete.min.js application/static/javascripts/ && cp node_modules/accessible-autocomplete/dist/accessible-autocomplete.min.css application/static/stylesheets/',
    govukJs: 'mkdir -p application/static/javascripts/govuk && cp node_modules/govuk-frontend/dist/govuk/all.bundle.js application/static/javascripts/govuk/govuk-frontend.js',
    // Convenience script to copy everything
    all: 'nps copy.javascripts copy.images copy.govukAssets copy.vendor copy.accessibleAutocomplete copy.govukJs'
  }
};

// Merge with digital-land-frontend scripts
module.exports = {
  scripts: {
    ...dlFrontendScripts.scripts,
    copy: customScripts.copy,
    // You can add completely new top-level scripts too
    setup: 'npm run copy.all && npm run build.stylesheets && npm run build.javascripts'
  }
};
