function ResourceCheck ($module) {
  this.$module = $module
}

ResourceCheck.prototype.init = function (params) {
  if (this.hasBeenChecked()) {
    this.display()
  } else {
    this.hide()
    this.performCheck()
  }

  return this
}

ResourceCheck.prototype.hasBeenChecked = function () {
  if (this.$module.dataset.checkPerformed === 'true') {
    return true
  }
  return false
}

ResourceCheck.prototype.performCheck = function () {
  const hash = this.$module.dataset.resourceHash
  const boundCheckCompleteHandler = this.checkCompleteHandler.bind(this)
  window.fetch(`/resource/${hash}/check`)
    .then(response => response.json())
    .then(boundCheckCompleteHandler)
}

ResourceCheck.prototype.checkCompleteHandler = function (data) {
  this.display()
}

ResourceCheck.prototype.display = function () {
  this.$module.classList.remove('js-hidden')
}

ResourceCheck.prototype.hide = function () {
  this.$module.classList.add('js-hidden')
}

export default ResourceCheck
