function SummaryMetrics(summary_metrics) {
    this.summary_metrics = summary_metrics;
}

SummaryMetrics.prototype.init = function () {
    console.log("hello from init function");
    const today = new Date();
    const date = today.toISOString().split('T')[0];
    const contributions_index = this.summary_metrics.contributions.dates.indexOf(date);
    const errors_index = this.summary_metrics.endpoint_errors.dates.indexOf(date);
    document.getElementById('contributions').innerHTML = "Contributions: ".concat(this.summary_metrics.contributions.contributions[contributions_index]);
    document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(this.summary_metrics.endpoint_errors.errors[errors_index]);



    return this
};

// SummaryMetrics.prototype.updateMetrics = function (e) {
//     console.log("hello from update function");
//     if (e != undefined) {
//     console.log(e.target.value);
//     console.log(this)
//     document.getElementById('summary-title').innerHTML = e.target.value.concat(" Summary");
//     const contributions_index = this.summary_metrics.contributions.dates.indexOf(e.target.value);
//     const errors_index =this.summary_metrics.endpoint_errors.dates.indexOf(e.target.value);
//     document.getElementById('contributions').innerHTML = "Contributions: ".concat(this.summary_metrics.contributions.contributions[contributions_index]);
//     document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(this.summary_metrics.endpoint_errors.errors[errors_index]);
//     }
// };

export { SummaryMetrics as default };
