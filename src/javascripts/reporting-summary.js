class SummaryMetrics {
    constructor(summary_metrics) {
        this.summary_metrics = summary_metrics;
        const dateFromSelector = document.querySelector("#date-from-selector")
        const dateToSelector = document.querySelector("#date-to-selector")
        dateFromSelector.addEventListener("change", this.updateMetrics.bind(this))
        dateToSelector.addEventListener("change", this.updateMetrics.bind(this))
    }

    init() {
        const today = new Date();
        const date = today.toISOString().split('T')[0];
        const contributions_index = this.summary_metrics.contributions.dates.indexOf(date);
        const errors_index = this.summary_metrics.endpoint_errors.dates.indexOf(date);
        document.getElementById('contributions').innerHTML = "Resources downloaded: ".concat(this.summary_metrics.contributions.contributions[contributions_index]);
        document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(this.summary_metrics.endpoint_errors.errors[errors_index]);
        document.getElementById('date-from-selector').value = date
        document.getElementById('date-to-selector').value = date
        return this
    }

    updateMetrics (e) {
            if (e != undefined) {
                const today = new Date();
                const date = today.toISOString().split('T')[0];

                const dateFromSelector = document.querySelector("#date-from-selector")
                let date_from = String(dateFromSelector.value)
                const dateToSelector = document.querySelector("#date-to-selector")
                let date_to = String(dateToSelector.value)

                if (!date_from) {
                    date_from = date
                }
                if (!date_to) {
                    date_to = date
                }

                // Find dates in lists so we know indexes of data
                let contributions_date_from_index = this.summary_metrics.contributions.dates.indexOf(date_from)
                let contributions_date_to_index = this.summary_metrics.contributions.dates.indexOf(date_to)
                // If date outside data range use max/min
                if (contributions_date_from_index == -1) {contributions_date_from_index = 0}
                if (contributions_date_to_index == -1) {contributions_date_to_index = Object.keys(this.summary_metrics.contributions.dates).length - 1}

                let errors_date_from_index = this.summary_metrics.endpoint_errors.dates.indexOf(date_from)
                let errors_date_to_index = this.summary_metrics.endpoint_errors.dates.indexOf(date_to)
                if (errors_date_from_index == -1) {errors_date_from_index = 0}
                if (errors_date_to_index == -1) {errors_date_to_index = Object.keys(this.summary_metrics.endpoint_errors.dates).length - 1}


                let total_contributions;
                let total_errors;
                let summary_text;
                if (date_from == date_to) {
                    total_contributions = this.summary_metrics.contributions.contributions[contributions_date_from_index]
                    total_errors = this.summary_metrics.endpoint_errors.errors[errors_date_from_index]
                    summary_text = "Today's Summary"
                } else {
                    total_contributions = this.summary_metrics.contributions.contributions.slice(contributions_date_from_index, contributions_date_to_index+1).reduce((a,b)=>a+b)
                    total_errors = this.summary_metrics.endpoint_errors.errors.slice(errors_date_from_index, errors_date_to_index+1).reduce((a,b)=>a+b)
                    summary_text = date_from.concat(" to ").concat(date_to).concat(" Summary")
                }

                document.getElementById('contributions').innerHTML = "Resources downloaded: ".concat(total_contributions);
                document.getElementById('errors').innerHTML = "Endpoint errors: ".concat(total_errors);
            }
    }
}

export { SummaryMetrics as default };
