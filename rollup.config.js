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
  }
]
