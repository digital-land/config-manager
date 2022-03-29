import CollapsibleSection from './components/collapsible-section'

const $collapsibles = document.querySelectorAll("[data-module='app-collapsible']")
$collapsibles.forEach(function ($el) {
  const $section = $el.querySelector('.dl-collapsible')
  console.log($section)
  const $triggers = $el.querySelector('.expanding-line-break')
  const collapsibleComponent = new CollapsibleSection($section, $triggers).init()
})
