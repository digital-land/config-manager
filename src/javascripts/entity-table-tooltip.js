// Lightweight tooltip for changed entity-table cells. Appends a single element
// to document.body so it is never clipped by the table's horizontal scroll
// container (which a pure-CSS or absolutely-positioned tooltip would be).

(function () {
  var SELECTOR = ".app-cell-changed";
  var tooltip = null;

  function ensureTooltip() {
    if (!tooltip) {
      tooltip = document.createElement("div");
      tooltip.className = "app-tooltip";
      tooltip.setAttribute("role", "tooltip");
      tooltip.hidden = true;
      document.body.appendChild(tooltip);
    }
    return tooltip;
  }

  function show(cell) {
    var text = cell.getAttribute("data-platform-value");
    if (!text) return;
    var el = ensureTooltip();
    el.textContent = text;
    el.hidden = false;
    position(cell);
  }

  function position(cell) {
    if (!tooltip || tooltip.hidden) return;
    var rect = cell.getBoundingClientRect();
    var top = rect.top - tooltip.offsetHeight - 8;
    // Flip below the cell if there isn't room above.
    if (top < 0) top = rect.bottom + 8;
    var left = rect.left;
    var maxLeft = window.innerWidth - tooltip.offsetWidth - 8;
    if (left > maxLeft) left = Math.max(8, maxLeft);
    tooltip.style.top = top + window.scrollY + "px";
    tooltip.style.left = left + window.scrollX + "px";
  }

  function hide() {
    if (tooltip) tooltip.hidden = true;
  }

  function closest(node) {
    while (node && node.nodeType === 1) {
      if (node.matches && node.matches(SELECTOR)) return node;
      node = node.parentElement;
    }
    return null;
  }

  document.addEventListener("mouseover", function (e) {
    var cell = closest(e.target);
    if (cell) show(cell);
  });
  document.addEventListener("mouseout", function (e) {
    if (closest(e.target)) hide();
  });
  document.addEventListener("focusin", function (e) {
    var cell = closest(e.target);
    if (cell) show(cell);
  });
  document.addEventListener("focusout", hide);
  window.addEventListener("scroll", hide, true);
})();
