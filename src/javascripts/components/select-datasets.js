/* global accessibleAutocomplete */

import { empty } from '../utilities/DOM-helpers'

function SelectDatasets ($module) {
  this.$module = $module
}

SelectDatasets.prototype.init = function (params) {
  this.setupOptions(params)
  this.selectedNames = []
  this.selectedValues = []
  this.selected = {}

  // register elements
  this.$input = this.$module.querySelector(this.options.inputSelector)
  this.$summaryContainer = this.$module.querySelector(this.options.containerSelector)
  this.$select = this.$module.querySelector(this.options.selectSelector)
  this.$summary = this.$module.querySelector(this.options.summarySelector)

  this.setInitalSelection()

  this.displayControls()
  this.setUpAutoComplete()

  const boundOnDeselectClickHandler = this.onDeselectClickHandler.bind(this)
  this.$summary.addEventListener('click', boundOnDeselectClickHandler)

  return this
}

SelectDatasets.prototype.addSelectedOption = function (name, dataset) {
  if (!Object.prototype.hasOwnProperty.call(this.selected, dataset)) {
    this.selected[dataset] = name
  }
  this.refreshUI()
}

SelectDatasets.prototype.createDeselectButton = function (name) {
  const $btn = document.createElement('button')
  $btn.classList.add(this.options.btnClass)
  $btn.textContent = 'x'
  const $hiddenSpan = document.createElement('span')
  $hiddenSpan.classList.add('govuk-visually-hidden')
  $hiddenSpan.textContent = `Deselect ${name}`
  $btn.appendChild($hiddenSpan)
  return $btn
}

SelectDatasets.prototype.createSelectedItem = function (dataset, name) {
  const $item = document.createElement('li')
  $item.classList.add(this.options.itemClass)
  $item.textContent = name
  $item.dataset.dataset = dataset
  $item.appendChild(this.createDeselectButton(name))
  return $item
}

SelectDatasets.prototype.deselect = function (dataset) {
  // remove from list of selected
  if (Object.prototype.hasOwnProperty.call(this.selected, dataset)) {
    delete this.selected[dataset]
  }
  this.refreshUI()
}

SelectDatasets.prototype.displayControls = function () {
  this.$input.classList.add('js-hidden')
  this.$summaryContainer.classList.remove('js-hidden')
}

SelectDatasets.prototype.displaySelected = function () {
  // remove all items
  empty(this.$summary)
  // repopulate with selected items
  const that = this
  for (const selectedDataset in this.selected) {
    that.$summary.appendChild(that.createSelectedItem(selectedDataset, that.selected[selectedDataset]))
  }
}

SelectDatasets.prototype.onDeselectClickHandler = function (e) {
  e.preventDefault()
  if (e.target.tagName === 'BUTTON') {
    const $item = e.target.closest('.' + this.options.itemClass)
    this.deselect($item.dataset.dataset)
  }
}

SelectDatasets.prototype.setUpAutoComplete = function () {
  const boundOptionSelectedHandler = this.optionSelectedHandler.bind(this)
  accessibleAutocomplete.enhanceSelectElement({
    selectElement: this.$select,
    onConfirm: boundOptionSelectedHandler,
    confirmOnBlur: false
  })
}

SelectDatasets.prototype.optionSelectedHandler = function (o) {
  const val = this.$select.querySelector(`[data-option-selector="${o}"]`).value
  this.addSelectedOption(o, val)
}

SelectDatasets.prototype.refreshUI = function () {
  this.updateInput()
  this.displaySelected()
}

SelectDatasets.prototype.setInitalSelection = function () {
  const datasetsString = this.$input.value
  const that = this
  if (datasetsString) {
    const datasets = datasetsString.split(';')
    datasets.forEach(function (dataset) {
      that.selectedValues.push(dataset)
      const $opt = that.$select.querySelector(`[value="${dataset}"]`)
      that.selectedNames.push($opt.textContent)
      that.selected[dataset] = $opt.textContent
    })
  }
  // display the initially selected datasets
  this.displaySelected()
}

SelectDatasets.prototype.setupOptions = function (params) {
  this.options = {
    btnClass: params.btnClass || 'app-select-datasets__btn',
    containerSelector: params.containerSelector || '.app-select-datasets__container',
    inputSelector: params.inputSelector || '.app-select-datasets__input',
    itemClass: params.itemClass || 'app-select-datasets__item',
    selectSelector: params.selectSelector || '.app-select-datasets__select',
    summarySelector: params.summarySelector || '.app-select-datasets__summary'
  }
}

SelectDatasets.prototype.updateInput = function () {
  const selectedDatasets = []
  for (const dataset in this.selected) {
    selectedDatasets.push(dataset)
  }
  this.$input.value = selectedDatasets.join(';')
}

// SelectDatasets.prototype.onlyUnique = function (value, index, self) {
//   return self.indexOf(value) === index
// }

SelectDatasets.prototype.getSelect = function () {
  return this.$select
}

export default SelectDatasets
