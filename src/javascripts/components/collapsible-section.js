function CollapsibleSection ($section, $trigger) {
  this.$section = $section
  this.$trigger = $trigger
}

CollapsibleSection.prototype.init = function () {
  if (!this.$section || !this.$trigger) {
    return undefined
  }

  const btns = this.getBtns()

  // show triggers
  this.$trigger.classList.remove('js-hidden')
  const boundClickHandler = this.clickHandler.bind(this)

  // make sure it is closed by default
  this.close()

  btns.forEach(function ($btn) {
    $btn.addEventListener('click', boundClickHandler)
  })

  const that = this
  this.$section.addEventListener('animationend', function (e) {
    if (e.animationName === 'opening-panel') {
      console.log('open animation')
      that.$section.classList.remove('opening')
      that.$section.dataset.collapsible = 'open'
    } else if (e.animationName === 'closing-panel') {
      console.log('close animation')
      that.$section.classList.remove('closing')
      that.$section.dataset.collapsible = 'closed'
    }
  })

  return this
}

CollapsibleSection.prototype.getBtns = function () {
  this.$openBtn = this.$trigger.querySelector('.open-section')
  this.$closeBtn = this.$trigger.querySelector('.close-section')

  return [this.$openBtn, this.$closeBtn]
}

CollapsibleSection.prototype.clickHandler = function (e) {
  const $clickedBtn = e.target
  if ($clickedBtn.dataset.action === 'open-section') {
    this.open()
  } else {
    this.close()
  }
}

CollapsibleSection.prototype.open = function (animate = true) {
  this.$openBtn.classList.add('js-hidden')
  this.$closeBtn.classList.remove('js-hidden')
  if (animate) {
    this.$section.classList.add('opening')
  } else {
    this.$section.dataset.collapsible = 'open'
  }
}

CollapsibleSection.prototype.close = function (animate = true) {
  this.$closeBtn.classList.add('js-hidden')
  this.$openBtn.classList.remove('js-hidden')
  if (animate) {
    this.$section.classList.add('closing')
  } else {
    this.$section.dataset.collapsible = 'closed'
  }
}

// CollapsibleSection.prototype.animationHandler = function(e) {
//   categoriesContainer.addEventListener('animationend', function(e) {
//     console.log(e);
//     if(e.animationName === "opening-panel") {
//       categoriesContainer.classList.remove('opening');
//       categoriesContainer.classList.remove('collapsed');
//     } else if (e.animationName === "closing-panel") {
//       categoriesContainer.classList.remove('closing');
//       categoriesContainer.classList.add('collapsed');
//     }
//   });
// }

export default CollapsibleSection
