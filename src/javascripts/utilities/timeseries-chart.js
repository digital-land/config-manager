class TimeseriesChart {
    constructor (options) {
        const {xAxisTitle, xAxisKey, yAxisTitle, yAxisKey, datasets, htmlId, type, stacked} = options
        this.xAxisTitle = xAxisTitle
        this.xAxisKey = xAxisKey
        this.yAxisTitle = yAxisTitle
        this.yAxisKey = yAxisKey
        this.datasets = datasets
        this.htmlId = htmlId
        this.type = type
        this.stacked = stacked
    }

    init() {
        const graph_ctx = document.getElementById(this.htmlId);

        return new Chart(graph_ctx, {
            type: this.type,
            data: {
            datasets: this.datasets
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
                    beginAtZero: true,
                    stacked: this.stacked
                    },
                    x: {
                        title: {
                            display: true,
                            text: this.xAxisTitle
                        },
                    stacked: this.stacked
                    }
                }
            }
        });
    }
}

export {TimeseriesChart as default}
