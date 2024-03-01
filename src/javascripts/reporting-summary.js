class SummaryMetrics {
    constructor(summary_metrics) {
        this.summary_metrics = summary_metrics;
        const dateSelector = document.querySelector("#date-selector")
        dateSelector.addEventListener("change", this.updateMetrics.bind(this))
    }

    init() {
        const today = new Date();
        const date = today.toISOString().split('T')[0];
        const contributions_index = this.summary_metrics.contributions.dates.indexOf(date);
        const errors_index = this.summary_metrics.endpoint_errors.dates.indexOf(date);
        document.getElementById('contributions').innerHTML = "Contributions: ".concat(this.summary_metrics.contributions.contributions[contributions_index]);
        document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(this.summary_metrics.endpoint_errors.errors[errors_index]);
        return this
    }

    updateMetrics (e) {
            if (e != undefined) {
                document.getElementById('summary-title').innerHTML = e.target.value.concat(" Summary");
                const contributions_index = this.summary_metrics.contributions.dates.indexOf(e.target.value);
                const errors_index =this.summary_metrics.endpoint_errors.dates.indexOf(e.target.value);
                document.getElementById('contributions').innerHTML = "Contributions: ".concat(this.summary_metrics.contributions.contributions[contributions_index]);
                document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(this.summary_metrics.endpoint_errors.errors[errors_index]);
            }
    }
}

export { SummaryMetrics as default };
