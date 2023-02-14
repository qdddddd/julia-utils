module SqlFunc
export agg_results, get_comp_df, get_prices

using DataFrames, ClickHouse, StatsBase

include("df_utils.jl")
import .DfUtils

include("common.jl")
import .CommonUtils.join_str

include("constants.jl")

function get_prices(conn, dates, ids)
    prices = select_df(conn,  """
        SELECT DISTINCT Symbol, Date, toFloat32(PreClose) PreClose, toFloat32(OpenPrice) OpenPrice, toFloat32(Price) EodPrice
        FROM $(input_tb)
        WHERE Id IN ($(join_str(ids))) AND Date IN ($(join_str(dates)))
        ORDER BY Date
    """);

    return prices
end

function agg_results(conn, dates, ids; with_real=false, prices=nothing)
    id_query = join_str(ids)

    if prices !== nothing
        eod_prices = combine(groupby(prices, :Symbol), :EodPrice => mean => :AvgEodPrice)
        price_bins = nquantile(eod_prices.AvgEodPrice, 10)
        eod_prices[!, :PriceBin] .= 0

        for i in 1:length(price_bins)-1
            lower = price_bins[i]
            upper = price_bins[i+1]

            if i < length(price_bins)-1
                eod_prices[eod_prices.AvgEodPrice .>= lower .&& eod_prices.AvgEodPrice .< upper, :PriceBin] .= i
            else
                eod_prices[eod_prices.AvgEodPrice .>= lower .&& eod_prices.AvgEodPrice .<= upper, :PriceBin] .= i
            end
        end

        execute(conn, """DROP TABLE IF EXISTS EodPrices""")
        execute(conn, """
            CREATE TEMPORARY TABLE EodPrices (
                Symbol FixedString(9),
                AvgEodPrice Float32,
                PriceBin Int
            )
        """);

        dict = Dict(pairs(eachcol(eod_prices)))
        insert(conn, "EodPrices", [dict])
    end

    query_results = """
    CREATE TEMPORARY TABLE result_compare AS
    SELECT * FROM (
        SELECT * FROM (
            SELECT InputHash, Portfolio, Symbol, Date, Capital, WeightedCapital, TheoModel,
                   Skewness, PositionLimitFactor, BiasFactor, Offset, BuyOffset, SellOffset,
                   OvnPriceFactor, OvnQtyFactor, UnitTradingAmount, UnitTradingVolume, HitLevel,
                   SeparateVirtualPos, Price, Inventory, Position,
                   LT.Pnl T0Pnl, OvernightPnl, IntradayPnl,
                   StaticPosition * (Price - PreClose) + T0Pnl Pnl,
                   Turnover, Fee, Strategy
                   $(ifelse(prices !== nothing, ",PriceBin", ""))
                   -- if(RunningFrom LIKE '%real_static%', 1, 0) RealStatic,
                   -- if(RunningFrom LIKE '%real_open%', 1, 0) RealOpen,
                   -- if(RunningFrom LIKE '%static_param%', 1, 0) StaticParam,
            FROM $(eod_tb) AS LT
            INNER JOIN (
                SELECT *, splitByChar('_', RunningFrom)[1] Strategy
                FROM $(input_tb) AS IT
                $(ifelse(prices !== nothing, """
                    LEFT JOIN (SELECT Symbol, PriceBin FROM EodPrices) AS PT
                    ON IT.Symbol = PT.Symbol
                """, ""))
                WHERE Id IN ($(id_query)) AND Date IN ($(join_str(dates)))
            ) AS RT
            ON LT.InputHash = RT.Id AND LT.Symbol = RT.Symbol AND LT.Date = RT.Date
        ) AS ET
        LEFT JOIN (
            SELECT *,
                   if(c = 1, 0, pbv) PrevBuyVolume,
                   if(c = 1, 0, psv) PrevSellVolume
            FROM (
                SELECT *,
                       count() OVER byHSD AS c,
                       first_value(BuyVolume) OVER byHSD AS pbv,
                       first_value(SellVolume) OVER byHSD AS psv
                FROM (
                    SELECT InputHash, toDate(Timestamp) Date, Symbol,
                           sumIf(Price * FilledVolume, Direction = 'B') BuyTurnover,
                           sumIf(Price * FilledVolume, Direction = 'S') SellTurnover,
                           sumIf(Price * FilledVolume, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenTurnover,
                           sumIf(Price * FilledVolume, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenTurnover,
                           sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) BuyCloseTurnover,
                           sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) SellCloseTurnover,
                           sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) BuyForceCloseTurnover,
                           sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) SellForceCloseTurnover,
                           sumIf(FilledVolume, Direction = 'B') BuyVolume,
                           sumIf(FilledVolume, Direction = 'S') SellVolume,
                           countIf(Direction = 'B') BuyTradesCount,
                           countIf(Direction = 'S') SellTradesCount,
                           sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenTradePnl,
                           sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenTradePnl,
                           sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) BuyCloseTradePnl,
                           sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) SellCloseTradePnl,
                           sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) BuyForceCloseTradePnl,
                           sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) SellForceCloseTradePnl
                    FROM $(trade_tb)
                    WHERE InputHash IN ($(id_query))
                    GROUP BY (InputHash, Date, Symbol)
                    ORDER BY Date
                )
                WINDOW byHSD AS (PARTITION BY (InputHash, Symbol) ORDER BY Date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
            )
        ) AS TT
        ON ET.InputHash = TT.InputHash AND ET.Date = TT.Date AND ET.Symbol = TT.Symbol
    ) AS BT
    """ * ifelse(with_real, """
    LEFT JOIN (
        SELECT Instrument Symbol,
               Date Date,
               Position PositionReal,
               PnlToday + PnlYesterday - Fee PnlReal,
               Turnover TurnoverReal,
               Fee FeeReal
        FROM hp.HpProfitByCode
        WHERE Class = 'ZYHB'
    ) AS RT
    ON BT.Symbol = RT.Symbol AND BT.Date = RT.Date
    """, "")
    execute(conn, "DROP TABLE IF EXISTS result_compare");
    execute(conn, query_results);
end;

function get_comp_df(conn; with_real=false, price_bin=false, parse_theo=false)
    query_by_dt = """
    SELECT InputHash,
           $(ifelse(price_bin, "", "--")) PriceBin,
           Date,
           -- groupArray(RealStatic)[1] RealStatc,
           -- groupArray(RealOpen)[1] RealOpen,
           -- groupArray(StaticParam)[1] StaticParam,
           groupArray(Portfolio)[1] Portfolio,
           groupArray(TheoModel)[1] TheoModel,
           groupArray(Strategy)[1] Strategy,
           groupArray(Capital)[1] Capital,
           groupArray(WeightedCapital)[1] WeightedCapital,
           groupArray(Skewness)[1] Skewness,
           groupArray(PositionLimitFactor)[1] PLF,
           groupArray(BiasFactor)[1] BiasFactor,
           groupArray(Offset)[1] AS Offset,
           groupArray(BuyOffset)[1] AS BuyOffset,
           groupArray(SellOffset)[1] AS SellOffset,
           groupArray(OvnPriceFactor)[1] AS OvnPriceFactor,
           groupArray(OvnQtyFactor)[1] AS OvnQtyFactor,
           groupArray(UnitTradingAmount)[1] AS UnitTradingAmount,
           groupArray(UnitTradingVolume)[1] AS UnitTradingVolume,
           groupArray(HitLevel)[1] AS HitLevel,
           groupArray(SeparateVirtualPos)[1] AS Sep,
           count(DISTINCT Symbol) AS NSymbols,
           if(Capital != 0, Capital*1e8, WeightedCapital*1e4*NSymbols) AS TotalCapital,
           sum((Inventory+Position) * Price) AS NetPositionValue,
           sum((Position) * Price) AS PositionValue,
           $(ifelse(with_real, "", "--")) sum(PositionReal * Price) AS PositionValueReal,
           $(ifelse(with_real, "", "--")) sum(PnlReal) AS PnlReal,
           sum(Pnl) AS Pnl,
           sum(T0Pnl) AS T0Pnl,
           sum(OvernightPnl) AS OvernightPnl,
           sum(IntradayPnl) AS IntradayPnl,
           sum(BuyOpenTradePnl) AS BuyOpenTradePnl,
           sum(SellOpenTradePnl) AS SellOpenTradePnl,
           sum(BuyCloseTradePnl) AS BuyCloseTradePnl,
           sum(SellCloseTradePnl) AS SellCloseTradePnl,
           sum(BuyForceCloseTradePnl) AS BuyForceCloseTradePnl,
           sum(SellForceCloseTradePnl) AS SellForceCloseTradePnl,
           $(ifelse(with_real, "", "--")) sum(TurnoverReal) AS TurnoverReal,
           sum(Turnover) AS Turnover,
           sum(BuyTurnover) AS BuyTurnover,
           sum(SellTurnover) AS SellTurnover,
           sum(BuyOpenTurnover) AS BuyOpenTurnover,
           sum(SellOpenTurnover) AS SellOpenTurnover,
           sum(BuyCloseTurnover) AS BuyCloseTurnover,
           sum(SellCloseTurnover) AS SellCloseTurnover,
           sum(BuyForceCloseTurnover) AS BuyForceCloseTurnover,
           sum(SellForceCloseTurnover) AS SellForceCloseTurnover,
           sum(BuyTradesCount) AS BuyTradesCount,
           sum(SellTradesCount) AS SellTradesCount,
           $(ifelse(with_real, "", "--")) sum(FeeReal) AS FeeReal,
           sum(Fee) AS Fee
    FROM result_compare
    GROUP BY Date, InputHash $(ifelse(price_bin, ", PriceBin", ""))
    """

    query = """
    SELECT InputHash,
           $(ifelse(price_bin, "", "--")) PriceBin,
           groupArray(Portfolio)[1] Portfolio,
           groupArray(Capital)[1] AS Capital,
           groupArray(WeightedCapital)[1] WeightedCapital,
           groupArray(Skewness)[1] AS Skewness,
           groupArray(PLF)[1] AS PLF,
           -- groupArray(RealStatic)[1] AS RealStatic,
           -- groupArray(RealOpen)[1] AS RealOpen,
           -- groupArray(StaticParam)[1] AS StaticParam,
           -- groupArray(Strategy)[1] AS Strategy,
           groupArray(TheoModel)[1] TheoModel,
           groupArray(BiasFactor)[1] AS BiasFactor,
           groupArray(Offset)[1] AS Offset,
           groupArray(BuyOffset)[1] AS BuyOffset,
           groupArray(SellOffset)[1] AS SellOffset,
           -- groupArray(OvnPriceFactor)[1] AS OvnPriceFactor,
           -- groupArray(OvnQtyFactor)[1] AS OvnQtyFactor,
           groupArray(UnitTradingAmount)[1] AS UnitTradingAmount,
           groupArray(UnitTradingVolume)[1] AS UnitTradingVolume,
           groupArray(HitLevel)[1] AS HitLevel,
           -- groupArray(Sep)[1] Sep,
           avg(NSymbols) AS NSymbols,
           avg(TotalCapital) AS TotalCapital,
           avg(NetPositionValue) AS NetPositionValue,
           avg(PositionValue) AS PositionValue,
           $(ifelse(with_real, "", "--")) avg(PositionValueReal) AS PositionValueReal,
           avg(Pnl) AS Pnl,
           avg(T0Pnl) AS T0Pnl,
           $(ifelse(with_real, "", "--")) avg(PnlReal) AS PnlReal,
           avg(OvernightPnl) AS OvernightPnl,
           avg(IntradayPnl) AS IntradayPnl,
           avg(BuyOpenTradePnl) AS BuyOpenTradePnl,
           avg(SellOpenTradePnl) AS SellOpenTradePnl,
           avg(BuyCloseTradePnl) AS BuyCloseTradePnl,
           avg(SellCloseTradePnl) AS SellCloseTradePnl,
           avg(BuyForceCloseTradePnl) AS BuyForceCloseTradePnl,
           avg(SellForceCloseTradePnl) AS SellForceCloseTradePnl,
           avg(Turnover) AS Turnover,
           avg(BuyTurnover) AS BuyTurnover,
           avg(SellTurnover) AS SellTurnover,
           avg(BuyOpenTurnover) AS BuyOpenTurnover,
           avg(SellOpenTurnover) AS SellOpenTurnover,
           avg(BuyCloseTurnover) AS BuyCloseTurnover,
           avg(SellCloseTurnover) AS SellCloseTurnover,
           avg(BuyForceCloseTurnover) AS BuyForceCloseTurnover,
           avg(SellForceCloseTurnover) AS SellForceCloseTurnover,
           avg(BuyTradesCount) AS BuyTradesCount,
           avg(SellTradesCount) AS SellTradesCount,
           $(ifelse(with_real, "", "--")) avg(TurnoverReal) TurnoverReal,
           avg(Fee) Fee
           $(ifelse(with_real, "", "--")) avg(FeeReal) FeeReal
    FROM ($(query_by_dt))
    GROUP BY InputHash $(ifelse(price_bin, ", PriceBin", ""))
    ORDER BY Capital, Offset
    """

    comp_date = select_df(conn, query_by_dt)
    sort!(comp_date, :Date);

    comp_avg = select_df(conn, query)

    if parse_theo
        comp_avg[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_avg.TheoModel, "_")]
        comp_date[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_date.TheoModel, "_")]
    end

    cols = [:InputHash,
            # :OvnPriceFactor, :OvnQtyFactor,
            :Portfolio, :Capital, :WeightedCapital, :NSymbols, :TotalCapital, :TheoModel,
            :Offset, :BuyOffset, :SellOffset, :Skewness, :UnitTradingAmount, :UnitTradingVolume, :BiasFactor,
            # :Strategy,
            :Pnl, :T0Pnl, :OvernightPnl, :IntradayPnl,
            :BuyOpenTradePnl, :SellOpenTradePnl, :BuyCloseTradePnl, :SellCloseTradePnl, :BuyForceCloseTradePnl, :SellForceCloseTradePnl,
            # :PnlReal,
            :PositionValue,
            :NetPositionValue,
            # :PositionValueReal,
            :Turnover, :BuyTurnover, :SellTurnover, :BuyOpenTurnover, :SellOpenTurnover, :BuyCloseTurnover, :SellCloseTurnover, :BuyForceCloseTurnover, :SellForceCloseTurnover,
            :BuyTradesCount, :SellTradesCount,
            # :TurnoverReal,
            :Fee,
            # :FeeReal
    ]

    if price_bin
        push!(cols, :PriceBin)
    end

    comp_avg = comp_avg[!, cols];
    comp_avg[!, :TradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    comp_avg[!, :BuyTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl;
    comp_avg[!, :SellTradePnl] = comp_avg.SellOpenTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    comp_avg[!, :OpenTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellOpenTradePnl;
    comp_avg[!, :CloseTradePnl] = comp_avg.BuyCloseTradePnl .+ comp_avg.SellCloseTradePnl;
    comp_avg[!, :ForceCloseTradePnl] = comp_avg.BuyForceCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    comp_avg[!, :TurnoverRatio] = comp_avg.Turnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :BuyTurnoverRatio] = comp_avg.BuyTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :SellTurnoverRatio] = comp_avg.SellTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :BuyOpenTurnoverRatio] = comp_avg.BuyOpenTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :SellOpenTurnoverRatio] = comp_avg.SellOpenTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :BuyCloseTurnoverRatio] = comp_avg.BuyCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :SellCloseTurnoverRatio] = comp_avg.SellCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :BuyForceCloseTurnoverRatio] = comp_avg.BuyForceCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :SellForceCloseTurnoverRatio] = comp_avg.SellForceCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    comp_avg[!, :BuySellTurnoverRatioDiff] = comp_avg.BuyTurnoverRatio .- comp_avg.SellTurnoverRatio;
    comp_avg[!, :NetPositionValueRatio] = comp_avg.NetPositionValue ./ (comp_avg.TotalCapital);
    comp_avg[!, :PositionValueRatio] = comp_avg.PositionValue ./ (comp_avg.TotalCapital);
    comp_avg[!, :T0Rtn] = comp_avg.T0Pnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :T0RtnOfTov] = comp_avg.T0Pnl ./ (comp_avg.Turnover);
    comp_avg[!, :OvernightRtn] = comp_avg.OvernightPnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :OvernightRtnOfTov] = comp_avg.OvernightPnl ./ (comp_avg.Turnover);
    comp_avg[!, :IntradayRtn] = comp_avg.IntradayPnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :IntradayRtnOfTov] = comp_avg.IntradayPnl ./ (comp_avg.Turnover);
    comp_avg[!, :IntradayInventoryRtn] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.TotalCapital);
    comp_avg[!, :IntradayInventoryRtnOfTov] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.Turnover);
    comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover);
    comp_avg[!, :TradeRtn] = comp_avg.TradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover);
    comp_avg[!, :BuyTradeRtn] = comp_avg.BuyTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :BuyTradeRtnOfTov] = comp_avg.BuyTradePnl ./ (comp_avg.BuyTurnover);
    comp_avg[!, :SellTradeRtn] = comp_avg.SellTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :SellTradeRtnOfTov] = comp_avg.SellTradePnl ./ (comp_avg.SellTurnover);
    comp_avg[!, :BuyOpenTradeRtn] = comp_avg.BuyOpenTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :BuyOpenTradeRtnOfTov] = comp_avg.BuyOpenTradePnl ./ (comp_avg.BuyTurnover);
    comp_avg[!, :SellOpenTradeRtn] = comp_avg.SellOpenTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :SellOpenTradeRtnOfTov] = comp_avg.SellOpenTradePnl ./ (comp_avg.SellTurnover);
    comp_avg[!, :BuyCloseTradeRtn] = comp_avg.BuyCloseTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :BuyCloseTradeRtnOfTov] = comp_avg.BuyCloseTradePnl ./ (comp_avg.BuyTurnover);
    comp_avg[!, :SellCloseTradeRtn] = comp_avg.SellCloseTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :SellCloseTradeRtnOfTov] = comp_avg.SellCloseTradePnl ./ (comp_avg.SellTurnover);
    comp_avg[!, :BuyForceCloseTradeRtn] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :BuyForceCloseTradeRtnOfTov] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.BuyTurnover);
    comp_avg[!, :SellForceCloseTradeRtn] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.TotalCapital);
    comp_avg[!, :SellForceCloseTradeRtnOfTov] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.SellTurnover);

    comp_date[!, :TradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.BuyForceCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    comp_date[!, :BuyTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.BuyForceCloseTradePnl;
    comp_date[!, :SellTradePnl] = comp_date.SellOpenTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    comp_date[!, :OpenTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellOpenTradePnl;
    comp_date[!, :CloseTradePnl] = comp_date.BuyCloseTradePnl .+ comp_date.SellCloseTradePnl;
    comp_date[!, :ForceCloseTradePnl] = comp_date.BuyForceCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    comp_date[!, :TurnoverRatio] = comp_date.Turnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :BuyOpenTurnoverRatio] = comp_date.BuyOpenTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :SellOpenTurnoverRatio] = comp_date.SellOpenTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :BuyCloseTurnoverRatio] = comp_date.BuyCloseTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :SellCloseTurnoverRatio] = comp_date.SellCloseTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :BuyForceCloseTurnoverRatio] = comp_date.BuyForceCloseTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :SellForceCloseTurnoverRatio] = comp_date.SellForceCloseTurnover ./ (comp_date.TotalCapital .* 2);
    comp_date[!, :NetPositionRatio] = comp_date.NetPositionValue ./ (comp_date.TotalCapital);
    comp_date[!, :IntradayInventoryRtn] = (comp_date.IntradayPnl .- comp_date.TradePnl) ./ (comp_date.TotalCapital);
    comp_date[!, :IntradayInventoryRtnOfTov] = (comp_date.IntradayPnl .- comp_date.TradePnl) ./ (comp_date.Turnover);
    comp_date[!, :TradeRtn] = comp_date.TradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :TradeRtnOfTov] = comp_date.TradePnl ./ (comp_date.Turnover);
    comp_date[!, :BuyTradeRtn] = comp_date.BuyTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyTradeRtnOfTov] = comp_date.BuyTradePnl ./ (comp_date.BuyTurnover);
    comp_date[!, :SellTradeRtn] = comp_date.SellTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellTradeRtnOfTov] = comp_date.SellTradePnl ./ (comp_date.SellTurnover);
    comp_date[!, :BuyOpenTradeRtn] = comp_date.BuyOpenTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyOpenTradeRtnOfTov] = comp_date.BuyOpenTradePnl ./ (comp_date.BuyTurnover);
    comp_date[!, :SellOpenTradeRtn] = comp_date.SellOpenTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellOpenTradeRtnOfTov] = comp_date.SellOpenTradePnl ./ (comp_date.SellTurnover);
    comp_date[!, :BuyCloseTradeRtn] = comp_date.BuyCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyCloseTradeRtnOfTov] = comp_date.BuyCloseTradePnl ./ (comp_date.BuyTurnover);
    comp_date[!, :SellCloseTradeRtn] = comp_date.SellCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellCloseTradeRtnOfTov] = comp_date.SellCloseTradePnl ./ (comp_date.SellTurnover);
    comp_date[!, :BuyForceCloseTradeRtn] = comp_date.BuyForceCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyForceCloseTradeRtnOfTov] = comp_date.BuyForceCloseTradePnl ./ (comp_date.BuyTurnover);
    comp_date[!, :SellForceCloseTradeRtn] = comp_date.SellForceCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellForceCloseTradeRtnOfTov] = comp_date.SellForceCloseTradePnl ./ (comp_date.SellTurnover);
    DfUtils.fillna!(comp_date.BuyTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.BuyCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.BuyForceCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellForceCloseTradeRtnOfTov, 0)

    return comp_date, comp_avg
end;

end
