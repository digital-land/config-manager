class TimeseriesChart {
    constructor (options) {
        const {xAxisTitle, xAxisKey, xMax, yAxisTitle, yAxisKey, yMax, datasets, htmlId, type, stacked} = options
        this.xAxisTitle = xAxisTitle
        this.xAxisKey = xAxisKey
        this.xMax = xMax
        this.yAxisTitle = yAxisTitle
        this.yAxisKey = yAxisKey
        this.yMax = yMax
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
                        text: this.yAxisTitle,
                    },
                    max: this.yMax,
                    beginAtZero: true,
                    stacked: this.stacked
                    },
                    x: {
                        title: {
                            display: true,
                            text: this.xAxisTitle
                        },
                    max: this.xMax,
                    stacked: this.stacked
                    }
                }
            }
        });
    }
}

export {TimeseriesChart as default}
