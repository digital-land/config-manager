/* global accessibleAutocomplete */

import { fetchWithArgs } from './utilities/fetchWithArgs'

// set up the auto complete inputs
accessibleAutocomplete.enhanceSelectElement({
  selectElement: document.querySelector('#organisation')
})

// accessibleAutocomplete.enhanceSelectElement({
//   selectElement: document.querySelector('#dataset')
// })

function appFetch (url, fetchParams, _callback) {
  fetchWithArgs(url, fetchParams)
    .then(data => {
      if (_callback) {
        _callback(data)
      } else {
        console.log(data) // JSON data parsed by `data.json()` call
      }
    })
    .catch(function (err) {
      console.log('Error', err.response.status)
    })
}

const endpointSearchUrl = '/endpoint/search'

const $endpointInputContainer = document.querySelector('[data-module="endpoint-input"]')
const $endpointInput = $endpointInputContainer.querySelector('input[name="endpoint_url"]')
const $endpointWarning = $endpointInputContainer.querySelector('.app-input__warning')
const $endpointWarningCount = $endpointWarning.querySelector('.app-input__warning__endpoint-count')
$endpointWarning.classList.add('js-hidden')
$endpointInput.addEventListener('input', function (e) {
  $endpointWarning.classList.add('js-hidden')
  console.log('on input should hide any info/warnings')
})

function handleReturnedEndpoint (data) {
  if (data) {
    $endpointWarning.classList.remove('js-hidden')
    console.log('handling response')
    console.log(data)
    const gram = (data.sources.length === 1) ? $endpointWarningCount.dataset.singular : $endpointWarningCount.dataset.plural;
    const message = `${data.sources.length} ${gram} this endpoint`
    $endpointWarningCount.textContent = message
  }
}

$endpointInput.addEventListener('change', function (e) {
  console.log('on change check if seen endpoint url before')
  const url = e.target.value
  appFetch(endpointSearchUrl, { endpoint_url: url.trim() }, handleReturnedEndpoint)
})
console.log($endpointInput)

window.appFetch = appFetch
