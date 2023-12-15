# Clickhouse table names
param_tb = "hp.ProdPrediction"
input_tb = "hp.BacktestInputs"
eod_tb = "hp.BacktestEodResults"
trade_tb = "hp.BacktestTrades"
order_tb = "hp.BacktestOrders"

# Set default plot template
index_codes = Dict(
    "50" => "000016.SH",
    "300" => "000300.SH",
    "500" => "000905.SH",
    "1000" => "000852.SH",
    "2000" => "399303.SZ",
    "WDQA" => "881001.WI",
    "ZZQZ" => "000985.CSI",
    "SZ" => "399106.SZ",
    "IH" => "000016.SH",
    "IF" => "000300.SH",
    "IC" => "000905.SH",
    "IM" => "000852.SH"
);

using Dates
holidays = Set(Date.(readlines(joinpath(@__DIR__, "holidays.txt"))))
