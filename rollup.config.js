module.exports = [
  {
    input: 'src/javascripts/application-add-source.js',
    output: {
      file: 'application/static/javascripts/application-add-source.js',
      format: 'iife'
    }
  },
  {
    input: 'src/javascripts/app-background-check.js',
    output: {
      file: 'application/static/javascripts/app-background-check.js',
      format: 'iife'
    }
  },
  {
    input: 'src/javascripts/app-resource-mapping.js',
    output: {
      file: 'application/static/javascripts/app-resource-mapping.js',
      format: 'iife'
    }
  },
  {
    input: 'src/javascripts/reporting-summary.js',
    output: {
      file: 'application/static/javascripts/reporting-summary.js',
      // format: 'iife'
    }
  },
  {
    input: 'src/javascripts/utilities/timeseries-chart.js',
    output: {
      file: 'application/static/javascripts/utilities/timeseries-chart.js',
      // format: 'iife'
    }
  },
  {
    input: 'src/javascripts/map.js',
    output: {
      file: 'application/static/javascripts/map.js',
      format: 'iife'
    }
  }
]
