import ResourceCheck from './components/resource-check'

const $mappingLink = document.querySelector('[data-module="resource-check"]')
const mappingModule = new ResourceCheck($mappingLink).init({})
