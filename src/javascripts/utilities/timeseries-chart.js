class TimeseriesChart {
    constructor (options) {
        const {xAxisTitle, xAxisKey, yAxisTitle, yAxisKey, label, data, htmlId} = options
        this.xAxisTitle = xAxisTitle
        this.xAxisKey = xAxisKey
        this.yAxisTitle = yAxisTitle
        this.yAxisKey = yAxisKey
        this.label = label
        this.data = data
        this.htmlId = htmlId
    }

    init() {
        const graph_ctx = document.getElementById(this.htmlId);

        return new Chart(graph_ctx, {
            type: 'line',
            data: {
            datasets: [{
                label: this.label,
                data: this.data,
                borderWidth: 1
            }]
            },
            options: {
                parsing: {
                    xAxisKey: this.xAxisKey,
                    yAxisKey: this.yAxisKey
                },
                scales: {
                    y: {
                    title: {
                        display: true,
                        text: this.yAxisTitle
                    },
                    beginAtZero: true
                    },
                    x: {
                        title: {
                            display: true,
                            text: this.xAxisTitle
                        }
                    }
                }
            }
        });
    }
}

export {TimeseriesChart as default}
