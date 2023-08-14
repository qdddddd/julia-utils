module SqlFunc
export get_table, get_input_hash, agg_results, get_comp_df, get_prices, get_trade_rtn

using DataFrames, StatsBase, DataFramesMeta, ClickHouse

include("df_utils.jl")
import .DfUtils

include("db_utils.jl")
import .DbUtils: conn

include("common.jl")
import .CommonUtils.join_str
import .CommonUtils.format_dt
import .CommonUtils.add_bins

include("constants.jl")

function get_st(symbol, date)
    select_df(conn(), """
        WITH '$(format_dt(date))' as dt
        SELECT * FROM winddb_mirror.asharest
        WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
            AND S_INFO_WINDCODE = '$(symbol)'
    """)
end;

function get_st(date)
    select_df(conn(), """
        WITH '$(format_dt(date))' as dt
        SELECT DISTINCT dt Date, S_INFO_WINDCODE Code FROM winddb_mirror.asharest
        WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
    """)
end;

function get_index_members(conn, date, index)
    index_code = index_codes[index]
    if endswith(index_code, "WI")
        table = "winddb_mirror.aindexmemberswind";
        index_col = "F_INFO_WINDCODE";
    else
        table = "winddb.aindexmembers";
        index_col = "S_INFO_WINDCODE";
    end

    select_df(conn, """
        WITH '$(format_dt(date))' AS dt
        SELECT DISTINCT S_CON_WINDCODE Code FROM $(table)
        WHERE $(index_col) = '$(index_code)'
            AND (S_CON_OUTDATE > dt OR S_CON_OUTDATE is NULL)
            AND S_CON_INDATE <= dt
            AND Code NOT IN (
                SELECT DISTINCT S_INFO_WINDCODE
                FROM winddb_mirror.asharest
                WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
            )
    """)
end

function get_apr_info(date, codes)
    select_df(conn(), """
        WITH '$(CommonUtils.format_dt(date))' AS dt
            SELECT Code,
                   OpenPrice,
                   ClosePrice,
                   PreClosePrice,
                   AdjFactor,
                   Amount,
                   TotalMarketValue,
                   FreeMarketValue
            FROM (
                -- 基本信息
                SELECT S_INFO_WINDCODE                    Code,
                       toDecimal64(S_DQ_OPEN, 4)          OpenPrice,
                       toDecimal64(S_DQ_CLOSE, 4)         ClosePrice,
                       toDecimal64(S_DQ_PRECLOSE, 4)      PreClosePrice,
                       toDecimal64(S_DQ_ADJFACTOR, 6)     AdjFactor,
                       toDecimal64(S_DQ_AMOUNT, 4) * 1000 Amount
                FROM winddb_mirror.ashareeodprices
                WHERE TRADE_DT = dt) AS TmpEodP
            JOIN (
                -- 查市值数据
                SELECT S_INFO_WINDCODE                  Code,
                       toDecimal64(S_VAL_MV, 4) * 10000 TotalMarketValue,
                       toDecimal64(S_DQ_MV, 4) * 10000  FreeMarketValue
                FROM winddb_mirror.ashareeodderivativeindicator
                WHERE TRADE_DT = dt) AS TmpEodC
            ON TmpEodP.Code = TmpEodC.Code
            WHERE abs(ClosePrice) > 0.0001 AND abs(PreClosePrice) > 0.0001
                AND Code IN ($(CommonUtils.join_str(codes)))
    """)
end;

function get_apr_simple_ver(date, rolling_window, codes)
    select_df(conn(), """
        WITH '$(CommonUtils.format_dt(date))' AS dt,
            $(rolling_window) AS rw,
            dates AS (
               SELECT DISTINCT TRADE_DT
               FROM winddb_mirror.ashareeodprices
               WHERE TRADE_DT <= dt AND S_DQ_AMOUNT > 0
               ORDER BY TRADE_DT DESC
               LIMIT rw
            ),
            dates100 AS (
               SELECT DISTINCT TRADE_DT
               FROM winddb_mirror.ashareeodprices
               WHERE TRADE_DT <= dt AND S_DQ_AMOUNT > 0
               ORDER BY TRADE_DT DESC
               LIMIT 100
            )
        SELECT Code,
              AvgAmount,
              AvgAmplitude,
              AvgTRTotal,
              AvgTRFree,
              AvgTotalMarketValue,
              AvgFreeMarketValue
        FROM (
            -- 历史平均成交额&振幅
            SELECT *, greatest(avgAmount, avgAmount100) AvgAmount
            FROM (
                SELECT S_INFO_WINDCODE                               Code,
                       avg(S_DQ_AMOUNT) * 1000                       avgAmount,
                       avg((S_DQ_HIGH - S_DQ_LOW) / S_DQ_LOW * 1000) AvgAmplitude
                FROM winddb_mirror.ashareeodprices
                WHERE TRADE_DT IN dates
                GROUP BY Code
            ) AS A
            JOIN (
                SELECT S_INFO_WINDCODE Code,
                       avg(S_DQ_AMOUNT) * 1000 avgAmount100
                FROM winddb_mirror.ashareeodprices
                WHERE TRADE_DT IN dates100
                GROUP BY Code
            ) AS B
            ON A.Code = B.Code
        ) AS TmpEodA
        JOIN (
           -- 历史平均市值&换手率
           SELECT S_INFO_WINDCODE        Code,
                  avg(S_DQ_TURN)         AvgTRTotal,
                  avg(S_DQ_FREETURNOVER) AvgTRFree,
                  avg(S_VAL_MV) * 10000  AvgTotalMarketValue,
                  avg(S_DQ_MV) * 10000   AvgFreeMarketValue
           FROM winddb_mirror.ashareeodderivativeindicator
           WHERE TRADE_DT IN dates
           GROUP BY Code
           ) AS TmpEodT
        ON TmpEodA.Code = TmpEodT.Code
        WHERE Code IN ($(CommonUtils.join_str(codes)))
    """)
end;

function get_table(conn, program_id, name; ih_col_name = "InputHash", cond = "")
    query = """
        SELECT * FROM $(name)
        WHERE $(ih_col_name) IN (
            SELECT DISTINCT Id FROM $(input_tb)
            WHERE RunningFrom LIKE '$(program_id)' $(cond)
        )
    """
    return select_df(conn, query)
end;

function get_input_hash(conn, program_id; cond=nothing)
    query = "SELECT DISTINCT Id FROM $(input_tb) WHERE RunningFrom LIKE '$(program_id)'"

    if cond !== nothing
        query *= " AND ($cond)"
    end

    res = select_df(conn, query)
    return nrow(res) == 0 ? String[] : res[:, :Id]
end;

function get_prices(conn, dates, ids; exchange=nothing)
    exchange_condition = exchange !== nothing ? "AND endsWith(Symbol, '$(exchange)')" : ""

    prices = select_df(conn,  """
        SELECT DISTINCT Symbol, Date, toFloat32(PreClose) PreClose, toFloat32(OpenPrice) OpenPrice, toFloat32(Price) EodPrice
        FROM $(input_tb)
        WHERE Id IN ($(join_str(ids))) AND Date IN ($(join_str(dates))) $(exchange_condition)
        ORDER BY Date
    """);

    return prices
end

function agg_results(conn, dates, ids; with_real=false, property=nothing, to_classify=nothing, tov=nothing, exchange=nothing, subset=nothing, pool=nothing)
    id_query = join_str(ids)

    if property !== nothing
        if exchange === nothing
            property_ = property
        else
            property_ = property[endswith.(property.Symbol, exchange), :]
        end

        add_bins!(property_, to_classify, n_bins=10)

        execute(conn, """DROP TABLE IF EXISTS Bins""")
        execute(conn, """
            CREATE TEMPORARY TABLE Bins (
                Symbol FixedString(9),
                $(to_classify) Float64,
                Bin Int
            )
        """);

        dict = Dict(pairs(eachcol(property_)))
        insert(conn, "Bins", [dict])
    end

    if tov !== nothing
        if exchange === nothing
            tov_ = tov
        else
            tov_ = tov[endswith.(tov.Symbol, exchange), :]
        end

        tov_bins = nquantile(tov_[tov_.:OpenPortion .> 0, :].OpenPortion, 10)
        tov_[!, :TovBin] .= 0

        for i in 1:length(tov_bins)-1
            lower = tov_bins[i]
            upper = tov_bins[i+1]

            if i == 1
                tov_[tov_.OpenPortion .< upper, :TovBin] .= i
            elseif i == length(tov_bins)-1
                tov_[tov_.OpenPortion .>= lower, :TovBin] .= i
            else
                tov_[tov_.OpenPortion .>= lower .&& tov_.OpenPortion .< upper, :TovBin] .= i
            end
        end

        execute(conn, """DROP TABLE IF EXISTS OpenPortions""")
        execute(conn, """
            CREATE TEMPORARY TABLE OpenPortions (
                Date Date,
                Symbol FixedString(9),
                OpenPortion Float32,
                TovBin Int
            )
        """);

        dict = Dict(pairs(eachcol(tov_[!, [:Date, :Symbol, :OpenPortion, :TovBin]])))
        insert(conn, "OpenPortions", [dict])
    end

    exchange_cond = exchange !== nothing ? "AND endsWith(Symbol, '$(exchange)')" : ""
    subset_cond = (
        subset !== nothing
        ? (
           startswith(subset, "AvgAmountRatio")
           ? "AND (toString(Symbol), Date) IN (SELECT DISTINCT Symbol, Date FROM tmp.AvgAmountRatios WHERE AvgAmountRatio_1_20 < $(split(subset, '#')[end]))"
           : "AND (toString(Symbol), Date) IN (SELECT DISTINCT Symbol, Date FROM tmp.$(subset))"
          )
        : ""
    )
    pool_cond = pool !== nothing ? "AND (toString(Symbol), Date) IN (SELECT DISTINCT Code Symbol, Date FROM hp.StockPools WHERE `$(pool)` = 1)" : ""

    query_results = """
    CREATE TEMPORARY TABLE result_compare AS
    SELECT * FROM (
        SELECT * FROM (
            SELECT InputHash, Portfolio, Symbol, Date, Capital, WeightedCapital, TheoModel,
                   Skewness, PositionLimitFactor, BiasFactor, Offset, BuyOffset, SellOffset, OffsetRange,
                   SkipFirstMinutes, OvnPriceFactor, OvnQtyFactor,
                   UnitTradingAmount, UnitTradingAmountPair, UnitTradingVolume, StopCriteria,
                   HitLevel, SeparateVirtualPos, Price, Inventory, Position,
                   LT.Pnl T0Pnl, OvernightPnl, IntradayPnl,
                   StaticPosition * (Price - PreClose) + T0Pnl Pnl,
                   Turnover, Fee, Latency, TradeWindow, NTradesPerSignal,
                   greatest(s0, s1) Strategy,
                   splitByChar('_', RunningFrom)[-1] PredLabel
                   $(ifelse(property !== nothing, ",Bin", ""))
                   $(ifelse(tov !== nothing, ",TovBin", ""))
                   -- if(RunningFrom LIKE '%real_static%', 1, 0) RealStatic,
                   -- if(RunningFrom LIKE '%real_open%', 1, 0) RealOpen,
                   -- if(RunningFrom LIKE '%static_param%', 1, 0) StaticParam,
            FROM $(eod_tb) AS LT
            INNER JOIN (
                SELECT *, REGEXP_EXTRACT(RunningFrom, '.*V\\d+_\\d+', 0) AS s0, REGEXP_EXTRACT(RunningFrom, '.*V\\d+', 0) AS s1
                FROM $(input_tb) AS IT
                $(ifelse(to_classify !== nothing, """
                    LEFT JOIN (SELECT Symbol, Bin FROM Bins) AS PT
                    ON IT.Symbol = PT.Symbol
                """, ""))
                $(ifelse(tov !== nothing, """
                    LEFT JOIN (SELECT Date, Symbol, OpenPortion, TovBin FROM OpenPortions) AS PT
                    ON IT.Symbol = PT.Symbol AND IT.Date = PT.Date
                """, ""))
                WHERE Id IN ($(id_query)) AND Date IN ($(join_str(dates)))
                    $(ifelse(tov !== nothing, "AND TovBin != 0", ""))
                    $(exchange_cond)
                    $(subset_cond)
                    $(pool_cond)
            ) AS RT
            ON LT.InputHash = RT.Id AND LT.Symbol = RT.Symbol AND LT.Date = RT.Date
            WHERE Symbol NOT IN ('000403.SZ', '000498.SZ', '300799.SZ', '000976.SZ', '000615.SZ', '300613.SZ', '300799.SZ', '000546.SZ', '000346.SZ', '000522.SZ')
        ) AS ET
        LEFT JOIN (
            SELECT InputHash, toDate(Timestamp) Date, Symbol,
                   sumIf(Price * FilledVolume, Direction = 'B') BuyTurnover,
                   sumIf(Price * FilledVolume, Direction = 'S') SellTurnover,
                   sumIf(Price * FilledVolume, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenTurnover,
                   sumIf(Price * FilledVolume, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenTurnover,
                   sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) BuyCloseTurnover,
                   sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) SellCloseTurnover,
                   sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) BuyForceCloseTurnover,
                   sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) SellForceCloseTurnover,
                   sumIf(EodPrice * FilledVolume, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenValue,
                   sumIf(EodPrice * FilledVolume, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenValue,
                   sumIf(EodPrice * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) BuyCloseValue,
                   sumIf(EodPrice * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) SellCloseValue,
                   sumIf(FilledVolume, Direction = 'B') BuyVolume,
                   sumIf(FilledVolume, Direction = 'S') SellVolume,
                   countIf(Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenCount,
                   countIf(Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenCount,
                   sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenTradePnl,
                   sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenTradePnl,
                   sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) BuyCloseTradePnl,
                   sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57) SellCloseTradePnl,
                   sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) BuyForceCloseTradePnl,
                   sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57) SellForceCloseTradePnl
            FROM $(trade_tb)
            WHERE InputHash IN ($(id_query)) AND Date IN ($(join_str(dates)))
                $(exchange_cond) $(subset_cond) $(pool_cond)
            GROUP BY (InputHash, Date, Symbol)
            ORDER BY Date
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
        WHERE Class = 'ZYHB' AND Date IN ($(join_str(dates)))
    ) AS RT
    ON BT.Symbol = RT.Symbol AND BT.Date = RT.Date
    """, "")
    execute(conn, "DROP TABLE IF EXISTS result_compare");
    execute(conn, query_results);

    if to_classify !== nothing
        return round.(bins, digits=3)
    end
end;

function get_comp_df(conn; with_real=false, bin=false, parse_theo=false, tov_bin=false, with_close=false)
    query_by_dt = """
    SELECT InputHash,
           $(ifelse(bin, "", "--")) Bin,
           $(ifelse(tov_bin, "", "--")) TovBin,
           Date,
           -- groupArray(RealStatic)[1] RealStatc,
           -- groupArray(RealOpen)[1] RealOpen,
           -- groupArray(StaticParam)[1] StaticParam,
           groupArray(Portfolio)[1] Portfolio,
           groupArray(TheoModel)[1] TheoModel,
           groupArray(Strategy)[1] Strategy,
           groupArray(PredLabel)[1] PredLabel,
           groupArray(Capital)[1] Capital,
           groupArray(WeightedCapital)[1] WeightedCapital,
           groupArray(Skewness)[1] Skewness,
           groupArray(PositionLimitFactor)[1] PLF,
           groupArray(BiasFactor)[1] BiasFactor,
           groupArray(Offset)[1] AS Offset,
           groupArray(BuyOffset)[1] AS BuyOffset,
           groupArray(SellOffset)[1] AS SellOffset,
           groupArray(OffsetRange)[1] AS OffsetRange,
           groupArray(SkipFirstMinutes)[1] AS SkipFirstMinutes,
           groupArray(OvnPriceFactor)[1] AS OvnPriceFactor,
           groupArray(OvnQtyFactor)[1] AS OvnQtyFactor,
           groupArray(UnitTradingAmount)[1] AS UnitTradingAmount,
           groupArray(UnitTradingAmountPair)[1] AS UnitTradingAmountPair,
           groupArray(UnitTradingVolume)[1] AS UnitTradingVolume,
           groupArray(StopCriteria)[1] AS StopCriteria,
           groupArray(HitLevel)[1] AS HitLevel,
           groupArray(SeparateVirtualPos)[1] AS Sep,
           groupArray(Latency)[1] AS Latency,
           groupArray(TradeWindow)[1] AS TradeWindow,
           groupArray(NTradesPerSignal)[1] AS NTradesPerSignal,
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
           sum(result_compare.BuyOpenTurnover) AS BuyOpenTurnover,
           sum(result_compare.SellOpenTurnover) AS SellOpenTurnover,
           sum(BuyCloseTurnover) AS BuyCloseTurnover,
           sum(SellCloseTurnover) AS SellCloseTurnover,
           sum(BuyForceCloseTurnover) AS BuyForceCloseTurnover,
           sum(SellForceCloseTurnover) AS SellForceCloseTurnover,
           sum(BuyOpenValue - SellOpenValue) AS NetBuyOpenValue,
           sum(SellOpenValue - BuyOpenValue) AS NetSellOpenValue,
           $(ifelse(with_close, "", "--")) if(NetBuyOpenValue = 0, 0, sum(SellCloseValue) / NetBuyOpenValue) AS BuyClosedPortion,
           $(ifelse(with_close, "", "--")) if(NetSellOpenValue = 0, 0, sum(BuyCloseValue) / NetSellOpenValue) AS SellClosedPortion,
           sum(BuyOpenCount) AS BuyOpenCount,
           sum(SellOpenCount) AS SellOpenCount,
           $(ifelse(with_real, "", "--")) sum(FeeReal) AS FeeReal,
           sum(Fee) AS Fee
    FROM result_compare
    GROUP BY Date, InputHash $(ifelse(bin, ", Bin", "")) $(ifelse(tov_bin, ", TovBin", ""))
    """

    query = """
    SELECT InputHash,
           $(ifelse(bin, "", "--")) Bin,
           $(ifelse(tov_bin, "", "--")) TovBin,
           groupArray(Portfolio)[1] Portfolio,
           groupArray(Capital)[1] AS Capital,
           groupArray(WeightedCapital)[1] WeightedCapital,
           groupArray(Skewness)[1] AS Skewness,
           groupArray(PLF)[1] AS PLF,
           -- groupArray(RealStatic)[1] AS RealStatic,
           -- groupArray(RealOpen)[1] AS RealOpen,
           -- groupArray(StaticParam)[1] AS StaticParam,
           groupArray(Strategy)[1] AS Strategy,
           groupArray(PredLabel)[1] AS PredLabel,
           groupArray(TheoModel)[1] TheoModel,
           groupArray(BiasFactor)[1] AS BiasFactor,
           groupArray(Offset)[1] AS Offset,
           groupArray(BuyOffset)[1] AS BuyOffset,
           groupArray(SellOffset)[1] AS SellOffset,
           groupArray(OffsetRange)[1] AS OffsetRange,
           groupArray(SkipFirstMinutes)[1] AS SkipFirstMinutes,
           -- groupArray(OvnPriceFactor)[1] AS OvnPriceFactor,
           -- groupArray(OvnQtyFactor)[1] AS OvnQtyFactor,
           groupArray(UnitTradingAmount)[1] AS UnitTradingAmount,
           groupArray(UnitTradingAmountPair)[1] AS UnitTradingAmountPair,
           groupArray(UnitTradingVolume)[1] AS UnitTradingVolume,
           groupArray(StopCriteria)[1] AS StopCriteria,
           groupArray(HitLevel)[1] AS HitLevel,
           groupArray(Latency)[1] AS Latency,
           groupArray(TradeWindow)[1] AS TradeWindow,
           groupArray(NTradesPerSignal)[1] AS NTradesPerSignal,
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
           $(ifelse(with_close, "", "--")) avg(BuyClosedPortion) AS BuyClosedPortion,
           $(ifelse(with_close, "", "--")) avg(SellClosedPortion) AS SellClosedPortion,
           avg(BuyOpenCount) AS BuyOpenCount,
           avg(SellOpenCount) AS SellOpenCount,
           $(ifelse(with_real, "", "--")) avg(TurnoverReal) TurnoverReal,
           avg(Fee) Fee
           $(ifelse(with_real, "", "--")) avg(FeeReal) FeeReal
    FROM ($(query_by_dt))
    GROUP BY InputHash $(ifelse(bin, ", Bin", "")) $(ifelse(tov_bin, ", TovBin", ""))
    ORDER BY Capital, Offset
    """

    comp_date = select_df(conn, query_by_dt)
    if nrow(comp_date) == 0
        return nothing
    end

    sort!(comp_date, :Date);

    comp_avg = select_df(conn, query)

    if parse_theo
        comp_avg[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_avg.TheoModel, "_")]
        comp_date[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_date.TheoModel, "_")]
    end

    cols = [:InputHash,
            # :OvnPriceFactor, :OvnQtyFactor,
            :Portfolio, :Capital, :WeightedCapital, :NSymbols, :TotalCapital, :TheoModel, :Latency, :TradeWindow, :NTradesPerSignal, :PLF,
            :Offset, :BuyOffset, :SellOffset, :OffsetRange, :SkipFirstMinutes,
            :Skewness, :UnitTradingAmount, :UnitTradingAmountPair, :UnitTradingVolume, :StopCriteria, :BiasFactor,
            :Strategy,
            :PredLabel,
            :Pnl, :T0Pnl, :OvernightPnl, :IntradayPnl,
            :BuyOpenTradePnl, :SellOpenTradePnl, :BuyCloseTradePnl, :SellCloseTradePnl, :BuyForceCloseTradePnl, :SellForceCloseTradePnl,
            # :PnlReal,
            :PositionValue,
            :NetPositionValue,
            # :PositionValueReal,
            :Turnover, :BuyTurnover, :SellTurnover, :BuyOpenTurnover, :SellOpenTurnover, :BuyCloseTurnover, :SellCloseTurnover, :BuyForceCloseTurnover, :SellForceCloseTurnover,
            :BuyOpenCount, :SellOpenCount,
            # :TurnoverReal,
            :Fee,
            # :FeeReal
    ]

    if with_close
        append!(cols, [:BuyClosedPortion, :SellClosedPortion])
    end

    if bin
        push!(cols, :Bin)
    end

    if tov_bin
        push!(cols, :TovBin)
    end

    comp_avg = comp_avg[!, cols];
    comp_avg[!, :TradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    if with_close
        comp_avg[!, :BuyTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
        comp_avg[!, :SellTradePnl] = comp_avg.SellOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl;
    else
        comp_avg[!, :BuyTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl;
        comp_avg[!, :SellTradePnl] = comp_avg.SellOpenTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    end
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
    comp_avg[!, :T0Rtn] = comp_avg.T0Pnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :T0RtnOfTov] = comp_avg.T0Pnl ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :OvernightRtn] = comp_avg.OvernightPnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :OvernightRtnOfTov] = comp_avg.OvernightPnl ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :IntradayRtn] = comp_avg.IntradayPnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :IntradayRtnOfTov] = comp_avg.IntradayPnl ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :IntradayInventoryRtn] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :IntradayInventoryRtnOfTov] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :TradeRtn] = comp_avg.TradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover) .* 260;
    comp_avg[!, :BuyTradeRtn] = comp_avg.BuyTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :BuyTradeRtnOfTov] = comp_avg.BuyTradePnl ./ (comp_avg.BuyOpenTurnover .+ comp_avg.SellCloseTurnover .+ comp_avg.SellForceCloseTurnover) .* 260;
    comp_avg[!, :SellTradeRtn] = comp_avg.SellTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :SellTradeRtnOfTov] = comp_avg.SellTradePnl ./ (comp_avg.SellOpenTurnover .+ comp_avg.BuyCloseTurnover .+ comp_avg.BuyForceCloseTurnover) .* 260;
    comp_avg[!, :BuyOpenTradeRtn] = comp_avg.BuyOpenTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :BuyOpenTradeRtnOfTov] = comp_avg.BuyOpenTradePnl ./ (comp_avg.BuyOpenTurnover) .* 260;
    comp_avg[!, :SellOpenTradeRtn] = comp_avg.SellOpenTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :SellOpenTradeRtnOfTov] = comp_avg.SellOpenTradePnl ./ (comp_avg.SellOpenTurnover) .* 260;
    comp_avg[!, :BuyCloseTradeRtn] = comp_avg.BuyCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :BuyCloseTradeRtnOfTov] = comp_avg.BuyCloseTradePnl ./ (comp_avg.BuyCloseTurnover) .* 260;
    comp_avg[!, :SellCloseTradeRtn] = comp_avg.SellCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :SellCloseTradeRtnOfTov] = comp_avg.SellCloseTradePnl ./ (comp_avg.SellCloseTurnover) .* 260;
    comp_avg[!, :BuyForceCloseTradeRtn] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :BuyForceCloseTradeRtnOfTov] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.BuyForceCloseTurnover) .* 260;
    comp_avg[!, :SellForceCloseTradeRtn] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    comp_avg[!, :SellForceCloseTradeRtnOfTov] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.SellForceCloseTurnover) .* 260;

    comp_date[!, :TradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.BuyForceCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    if with_close
        comp_date[!, :BuyTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
        comp_date[!, :SellTradePnl] = comp_date.SellOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.BuyForceCloseTradePnl;
    else
        comp_date[!, :BuyTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.BuyForceCloseTradePnl;
        comp_date[!, :SellTradePnl] = comp_date.SellOpenTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    end
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
    comp_date[!, :SellTradeRtn] = comp_date.SellTradePnl ./ (comp_date.TotalCapital);
    if with_close
        comp_date[!, :BuyTradeRtnOfTov] = comp_date.BuyTradePnl ./ (comp_date.BuyOpenTurnover .+ comp_date.SellCloseTurnover .+ comp_date.SellForceCloseTurnover);
        comp_date[!, :SellTradeRtnOfTov] = comp_date.SellTradePnl ./ (comp_date.SellOpenTurnover .+ comp_date.BuyCloseTurnover .+ comp_date.BuyForceCloseTurnover);
    else
        comp_date[!, :BuyTradeRtnOfTov] = comp_date.BuyTradePnl ./ (comp_date.BuyOpenTurnover .+ comp_date.BuyCloseTurnover .+ comp_date.BuyForceCloseTurnover);
        comp_date[!, :SellTradeRtnOfTov] = comp_date.SellTradePnl ./ (comp_date.SellOpenTurnover .+ comp_date.SellCloseTurnover .+ comp_date.SellForceCloseTurnover);
    end
    comp_date[!, :BuyOpenTradeRtn] = comp_date.BuyOpenTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyOpenTradeRtnOfTov] = comp_date.BuyOpenTradePnl ./ (comp_date.BuyOpenTurnover);
    comp_date[!, :SellOpenTradeRtn] = comp_date.SellOpenTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellOpenTradeRtnOfTov] = comp_date.SellOpenTradePnl ./ (comp_date.SellOpenTurnover);
    comp_date[!, :BuyCloseTradeRtn] = comp_date.BuyCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyCloseTradeRtnOfTov] = comp_date.BuyCloseTradePnl ./ (comp_date.BuyCloseTurnover);
    comp_date[!, :SellCloseTradeRtn] = comp_date.SellCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellCloseTradeRtnOfTov] = comp_date.SellCloseTradePnl ./ (comp_date.SellCloseTurnover);
    comp_date[!, :BuyForceCloseTradeRtn] = comp_date.BuyForceCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :BuyForceCloseTradeRtnOfTov] = comp_date.BuyForceCloseTradePnl ./ (comp_date.BuyForceCloseTurnover);
    comp_date[!, :SellForceCloseTradeRtn] = comp_date.SellForceCloseTradePnl ./ (comp_date.TotalCapital);
    comp_date[!, :SellForceCloseTradeRtnOfTov] = comp_date.SellForceCloseTradePnl ./ (comp_date.SellForceCloseTurnover);
    DfUtils.fillna!(comp_date.BuyTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.BuyCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.BuyForceCloseTradeRtnOfTov, 0)
    DfUtils.fillna!(comp_date.SellForceCloseTradeRtnOfTov, 0)

    return comp_date, comp_avg
end;

function get_trade_rtn(conn, date, ih, capital)
    select_df(conn, """
        SELECT InputHash, toDate(Timestamp) Date, Symbol,
               sumIf(Price * FilledVolume, Direction = 'B')/$capital/2 BuyTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S')/$capital/2 SellTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital/2 BuyOpenTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital/2 SellOpenTurnoverRatio,
               BuyOpenTurnoverRatio + SellOpenTurnoverRatio OpenTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital/2 BuyCloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital/2 SellCloseTurnoverRatio,
               BuyCloseTurnoverRatio + SellCloseTurnoverRatio CloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital/2 BuyForceCloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital/2 SellForceCloseTurnoverRatio,
               BuyForceCloseTurnoverRatio + SellForceCloseTurnoverRatio ForceCloseTurnoverRatio,
               sumIf(FilledVolume, Direction = 'B') BuyVolume,
               sumIf(FilledVolume, Direction = 'S') SellVolume,
               countIf(Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenCount,
               countIf(Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenCount,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital BuyOpenTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital SellOpenTradeRtn,
               BuyOpenTradeRtn + SellOpenTradeRtn OpenTradeRtn,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital BuyCloseTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital SellCloseTradeRtn,
               BuyCloseTradeRtn + SellCloseTradeRtn CloseTradeRtn,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital BuyForceCloseTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital SellForceCloseTradeRtn,
               BuyForceCloseTradeRtn + SellForceCloseTradeRtn ForceCloseTradeRtn
        FROM $(trade_tb)
        WHERE InputHash = '$ih' AND Date = '$date'
        GROUP BY (InputHash, Date, Symbol)
        ORDER BY Symbol
    """)
end;

function get_trade_rtn(conn, ih, capital)
    select_df(conn, """
        SELECT InputHash, toDate(Timestamp) Date, Symbol,
               sumIf(Price * FilledVolume, Direction = 'B')/$capital BuyTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S')/$capital SellTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital/2 BuyOpenTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital/2 SellOpenTurnoverRatio,
               BuyOpenTurnoverRatio + SellOpenTurnoverRatio OpenTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital/2 BuyCloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital/2 SellCloseTurnoverRatio,
               BuyCloseTurnoverRatio + SellCloseTurnoverRatio CloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital/2 BuyForceCloseTurnoverRatio,
               sumIf(Price * FilledVolume, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital/2 SellForceCloseTurnoverRatio,
               BuyForceCloseTurnoverRatio + SellForceCloseTurnoverRatio ForceCloseTurnoverRatio,
               sumIf(FilledVolume, Direction = 'B') BuyVolume,
               sumIf(FilledVolume, Direction = 'S') SellVolume,
               countIf(Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) BuyOpenCount,
               countIf(Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45)) SellOpenCount,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital BuyOpenTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND (toHour(Timestamp) < 14 OR toMinute(Timestamp) < 45))/$capital SellOpenTradeRtn,
               BuyOpenTradeRtn + SellOpenTradeRtn OpenTradeRtn,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital BuyCloseTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) >= 45 AND toMinute(Timestamp) < 57)/$capital SellCloseTradeRtn,
               BuyCloseTradeRtn + SellCloseTradeRtn CloseTradeRtn,
               sumIf((EodPrice - Price) * FilledVolume - Fee, Direction = 'B' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital BuyForceCloseTradeRtn,
               sumIf((EodPrice - Price) * -FilledVolume - Fee, Direction = 'S' AND toHour(Timestamp) = 14 AND toMinute(Timestamp) = 57)/$capital SellForceCloseTradeRtn,
               BuyForceCloseTradeRtn + SellForceCloseTradeRtn ForceCloseTradeRtn
        FROM $(trade_tb)
        WHERE InputHash = '$ih'
        GROUP BY (InputHash, Date, Symbol)
        ORDER BY Symbol
    """)
end;

end
