# Clickhouse table names
param_tb = "hp.ProdPrediction"
input_tb = "hp.BacktestInputs"
eod_tb = "hp.BacktestEodResults"
trade_tb = "hp.BacktestTrades"
order_tb = "hp.BacktestOrders"

using PlotlyJS
# Set default plot template
plot_template = plot().plot.layout.template
plot_template.layout.plot_bgcolor = :white
plot_template.layout.xaxis[:gridcolor] = "#E5ECF6"
plot_template.layout.xaxis[:zerolinecolor] = "#E5ECF6"
plot_template.layout.xaxis[:linecolor] = "#E5ECF6"
plot_template.layout.yaxis[:gridcolor] = "#E5ECF6"
plot_template.layout.yaxis[:zerolinecolor] = "#E5ECF6"
plot_template.layout.yaxis[:linecolor] = "#E5ECF6"
plot_template.layout.width=1460
plot_template.layout.height=500
plot_template.layout.paper_bgcolor=:white
