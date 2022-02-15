/* global accessibleAutocomplete */

accessibleAutocomplete.enhanceSelectElement({
  defaultValue: 'Start typing organisation name',
  selectElement: document.querySelector('#organisation')
})

accessibleAutocomplete.enhanceSelectElement({
  defaultValue: 'Start typing dataset name',
  selectElement: document.querySelector('#dataset')
})
