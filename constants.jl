# Clickhouse table names
param_tb = "hp.ProdPrediction"
input_tb = "hp.BacktestInputs"
eod_tb = "hp.BacktestEodResults"
trade_tb = "hp.BacktestTrades"
order_tb = "hp.BacktestOrders"

using PlotlyJS
# Set default plot template
default_grid_color = "#E5ECF6"
plot_template = PlotlyJS.plot().plot.layout.template
plot_template.layout.plot_bgcolor = :white
plot_template.layout.xaxis[:gridcolor] = default_grid_color
plot_template.layout.xaxis[:zerolinecolor] = default_grid_color
plot_template.layout.xaxis[:linecolor] = default_grid_color
plot_template.layout.yaxis[:gridcolor] = default_grid_color
plot_template.layout.yaxis[:zerolinecolor] = default_grid_color
plot_template.layout.yaxis[:linecolor] = default_grid_color
plot_template.layout.paper_bgcolor=:white
# plot_template.layout.width=1460
# plot_template.layout.height=500

using ColorSchemes
default_color = ColorSchemes.Blues[6]

index_codes = Dict(
    "50" => "000016.SH",
    "300" => "000300.SH",
    "500" => "000905.SH",
    "1000" => "000852.SH",
    "2000" => "399303.SZ",
    "WDQA" => "881001.WI",
    "ZZQZ" => "000985.CSI",
    "SZ" => "399106.SZ",
    "IF" => "000300.SH",
    "IC" => "000905.SH",
    "IM" => "000852.SH"
);
