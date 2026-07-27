module.exports = [
  {
    input: "src/javascripts/map.js",
    output: {
      file: "application/static/javascripts/map.js",
      format: "iife",
    },
  },
  {
    input: "src/javascripts/entity-table-tooltip.js",
    output: {
      file: "application/static/javascripts/entity-table-tooltip.js",
      format: "iife",
    },
  },
];
