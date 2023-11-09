module SqlFunc
export get_table, get_input_hash, agg_results, get_comp_df, get_prices, get_trade_rtn

using DataFrames, StatsBase, DataFramesMeta, ClickHouse

include("df_utils.jl")
import .DfUtils
import .DfUtils.add_bins!

include("db_utils.jl")
import .DbUtils: conn

include("common.jl")
import .CommonUtils.join_str
import .CommonUtils.format_dt

include("constants.jl")

function get_st(symbol, date)
    select_df(
        conn(),
        """
    WITH '$(format_dt(date))' as dt
    SELECT * FROM winddb_mirror.asharest FINAL
    WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
        AND S_INFO_WINDCODE = '$(symbol)'
"""
    )
end

function get_st(date)
    select_df(
        conn(),
        """
    WITH '$(format_dt(date))' as dt
    SELECT DISTINCT dt Date, S_INFO_WINDCODE Code FROM winddb_mirror.asharest FINAL
    WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
"""
    )
end

function get_index_members(conn, date, index, include_st=false)
    index_code = index_codes[index]
    if endswith(index_code, "WI")
        table = "winddb_mirror.aindexmemberswind FINAL"
        index_col = "F_INFO_WINDCODE"
    else
        table = "winddb_mirror.aindexmembers FINAL"
        index_col = "S_INFO_WINDCODE"
    end

    select_df(
        conn,
        """
    WITH '$(format_dt(date))' AS dt
    SELECT DISTINCT S_CON_WINDCODE Code FROM $(table)
    WHERE $(index_col) = '$(index_code)'
        AND (S_CON_OUTDATE > dt OR S_CON_OUTDATE is NULL)
        AND S_CON_INDATE <= dt
        $(include_st ? "" : """
        AND Code NOT IN (
            SELECT DISTINCT S_INFO_WINDCODE
            FROM winddb_mirror.asharest FINAL
            WHERE ENTRY_DT <= dt AND (REMOVE_DT > dt OR REMOVE_DT is NULL) AND S_TYPE_ST != 'R'
        )
        """)
"""
    )
end

function get_apr_info(date, codes, c=nothing)
    if c === nothing
        c = conn()
    end
    select_df(
        c,
        """
    WITH '$(CommonUtils.format_dt(date))' AS dt
        SELECT Code, OpenPrice, ClosePrice, PreClosePrice, AdjFactorRolling, Amount, Volume, TotalMarketValue, FreeMarketValue,
               (1 + ifNull(StrikeRate, 0) + ifNull(CashRate, 0) / OpenPrice) AdjFactor
        FROM (
            SELECT * FROM (
                -- 基本信息
                SELECT S_INFO_WINDCODE                    Code,
                       toDecimal64(S_DQ_OPEN, 4)          OpenPrice,
                       toDecimal64(S_DQ_CLOSE, 4)         ClosePrice,
                       toDecimal64(S_DQ_PRECLOSE, 4)      PreClosePrice,
                       toDecimal64(S_DQ_ADJFACTOR, 6)     AdjFactorRolling,
                       toDecimal64(S_DQ_AMOUNT, 4) * 1000 Amount,
                       toInt64(S_DQ_VOLUME * 100)         Volume
                FROM winddb_mirror.ashareeodprices FINAL
                WHERE TRADE_DT = dt) AS TmpEodP
            JOIN (
                -- 查市值数据
                SELECT S_INFO_WINDCODE                  Code,
                       toDecimal64(S_VAL_MV, 4) * 10000 TotalMarketValue,
                       toDecimal64(S_DQ_MV, 4) * 10000  FreeMarketValue
                FROM winddb_mirror.ashareeodderivativeindicator FINAL
                WHERE TRADE_DT = dt) AS TmpEodC
            ON TmpEodP.Code = TmpEodC.Code
            WHERE abs(ClosePrice) > 0.0001 AND abs(PreClosePrice) > 0.0001
                AND Code IN ($(CommonUtils.join_str(codes)))
        ) AS ET
        LEFT JOIN (
            SELECT EX_DT Date, WIND_CODE Code, CASH_DVD_PER_SH_AFTER_TAX CashRate, STK_DVD_PER_SH StrikeRate
            FROM winddb_mirror.asharedividend FINAL
            WHERE Date = dt
        ) AS DT
        ON ET.Code = DT.Code
"""
    )
end

function get_apr_simple_ver(date, rolling_window, codes)
    select_df(
        conn(),
        """
    WITH '$(CommonUtils.format_dt(date))' AS dt,
        $(rolling_window) AS rw,
        dates AS (
           SELECT DISTINCT TRADE_DT
           FROM winddb_mirror.ashareeodprices FINAL
           WHERE TRADE_DT <= dt AND S_DQ_AMOUNT > 0
           ORDER BY TRADE_DT DESC
           LIMIT rw
        ),
        dates100 AS (
           SELECT DISTINCT TRADE_DT
           FROM winddb_mirror.ashareeodprices FINAL
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
            FROM winddb_mirror.ashareeodprices FINAL
            WHERE TRADE_DT IN dates
            GROUP BY Code
        ) AS A
        JOIN (
            SELECT S_INFO_WINDCODE Code,
                   avg(S_DQ_AMOUNT) * 1000 avgAmount100
            FROM winddb_mirror.ashareeodprices FINAL
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
       FROM winddb_mirror.ashareeodderivativeindicator FINAL
       WHERE TRADE_DT IN dates
       GROUP BY Code
       ) AS TmpEodT
    ON TmpEodA.Code = TmpEodT.Code
    WHERE Code IN ($(CommonUtils.join_str(codes)))
"""
    )
end

function get_table(conn, program_id, name; ih_col_name="InputHash", cond="")
    query = """
        SELECT * FROM $(name)
        WHERE $(ih_col_name) IN (
            SELECT DISTINCT Id FROM $(input_tb)
            WHERE RunningFrom LIKE '$(program_id)' $(cond)
        )
    """
    return select_df(conn, query)
end

function get_input_hash(conn, program_id; cond=nothing)
    query = "SELECT DISTINCT Id FROM $(input_tb) WHERE RunningFrom LIKE '$(program_id)'"

    if cond !== nothing
        query *= " AND ($cond)"
    end

    res = select_df(conn, query)
    return nrow(res) == 0 ? String[] : res[:, :Id]
end

function get_prices(conn, dates, ids; exchange=nothing)
    exchange_condition = exchange !== nothing ? "AND endsWith(Symbol, '$(exchange)')" : ""

    prices = select_df(
        conn,
        """
    SELECT DISTINCT Symbol, Date, toFloat32(PreClose) PreClose, toFloat32(OpenPrice) OpenPrice, toFloat32(Price) EodPrice
    FROM $(input_tb)
    WHERE Id IN ($(join_str(ids))) AND Date IN ($(join_str(dates))) $(exchange_condition)
    ORDER BY Date
"""
    )

    return prices
end

function agg_results(conn, dates, ids; with_real=false, property=nothing, to_classify=nothing, tov=nothing, exchange=nothing, subset=nothing, pool=nothing, bin=false, parse_theo=false, tov_bin=false, with_close=false)
    id_query = join_str(ids)

    if property !== nothing
        if exchange === nothing
            property_ = property
        else
            property_ = property[endswith.(property.Symbol, exchange), :]
        end

        add_bins!(property_, to_classify, n_bins=10)

        execute(conn, """DROP TABLE IF EXISTS Bins""")
        execute(
            conn,
            """
    CREATE TEMPORARY TABLE Bins (
        Symbol FixedString(9),
        $(to_classify) Float64,
        Bin Int
    )
"""
        )

        dict = Dict(pairs(eachcol(property_)))
        insert(conn, "Bins", [dict])
    end

    if tov !== nothing
        if exchange === nothing
            tov_ = tov
        else
            tov_ = tov[endswith.(tov.Symbol, exchange), :]
        end

        tov_bins = nquantile(tov_[tov_.:OpenPortion.>0, :].OpenPortion, 10)
        tov_[!, :TovBin] .= 0

        for i in 1:length(tov_bins)-1
            lower = tov_bins[i]
            upper = tov_bins[i+1]

            if i == 1
                tov_[tov_.OpenPortion.<upper, :TovBin] .= i
            elseif i == length(tov_bins) - 1
                tov_[tov_.OpenPortion.>=lower, :TovBin] .= i
            else
                tov_[tov_.OpenPortion.>=lower.&&tov_.OpenPortion.<upper, :TovBin] .= i
            end
        end

        execute(conn, """DROP TABLE IF EXISTS OpenPortions""")
        execute(
            conn,
            """
    CREATE TEMPORARY TABLE OpenPortions (
        Date Date,
        Symbol FixedString(9),
        OpenPortion Float32,
        TovBin Int
    )
"""
        )

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

    query_by_dt = """
    WITH ($(id_query)) AS ids,
         dates AS (SELECT DISTINCT Date FROM hp.StockPools WHERE Date IN ($(join_str(dates)))),
         POOL AS (
         $(pool === nothing ? """
           SELECT DISTINCT Symbol, Date, true TodayIn, true PrevDayIn, Price EodPrice, PreClose, OpenPrice FROM $(input_tb)
           WHERE Id IN ids AND Date IN dates
         """ : """
            SELECT * FROM
            (
                SELECT DISTINCT Code Symbol, Date, TodayIn, PrevDayIn FROM
                (
                    SELECT Date, Code,
                           first_value(`$(pool)` = 1) OVER Prev As PrevDayIn,
                           last_value(`$(pool)` = 1) OVER Prev AS TodayIn
                    FROM hp.StockPools
                    WHERE Date IN dates
                    WINDOW Prev AS ( PARTITION BY Code ORDER BY Date ROWS BETWEEN 1 PRECEDING AND CURRENT ROW )
                )
                WHERE (PrevDayIn AND NOT TodayIn) OR TodayIn
            ) AS L
            INNER JOIN (
                SELECT S_INFO_WINDCODE Symbol,
                       TRADE_DT Date,
                       S_DQ_CLOSE EodPrice,
                       S_DQ_PRECLOSE PreClose,
                       S_DQ_OPEN OpenPrice
                FROM winddb_mirror.ashareeodprices FINAL
                WHERE Date IN dates
            ) AS R
            ON L.Date = R.Date AND L.Symbol = R.Symbol
        """)
         )
    SELECT * FROM (
        SELECT * FROM (
            SELECT * FROM (
                SELECT InputHash, Date,
                       $(bin ? "" : "--") Bin,
                       $(tov_bin ? "" : "--") TovBin,
                       sum((Inventory+Position) * EodPrice) AS NetPositionValue,
                       sum((Position) * EodPrice) AS PositionValue,
                       sum(Pnl) AS T0Pnl,
                       sum(OvernightPnl) AS OvernightPnl,
                       sum(IntradayPnl) AS IntradayPnl,
                       sum(Turnover) AS Turnover,
                       sum(Fee) AS Fee
                FROM (
                    SELECT *
                           $(property !== nothing ? ", Bin" : "")
                           $(tov !== nothing ? ", TovBin" : "")
                    FROM (
                        SELECT *,
                               ET.Inventory * (EodPrice - OpenPrice) IntradayInventoryPnl,
                               if(PrevDayIn, ET.OvernightPnl, 0) OvernightPnl,
                               if(PrevDayIn AND TodayIn, ET.Inventory, 0) Inventory,
                               if(TodayIn, ET.Position, 0) Position,
                               if(TodayIn, if(PrevDayIn, ET.IntradayPnl, ET.IntradayPnl - IntradayInventoryPnl), 0) IntradayPnl,
                               if(TodayIn, ET.Turnover, 0) Turnover,
                               if(TodayIn, ET.Fee, 0) Fee,
                               if(TodayIn, if(PrevDayIn, ET.Pnl, ET.Pnl - IntradayInventoryPnl - ET.OvernightPnl), ET.OvernightPnl) T0Pnl
                        FROM $(eod_tb) AS ET
                        $(pool === nothing ? "" : """
                            INNER JOIN POOL
                            ON ET.Symbol = POOL.Symbol AND ET.Date = POOL.Date
                        """)
                        WHERE InputHash IN ids AND Date IN dates $(exchange_cond) $(subset_cond)
                    ) AS ET
                    $(to_classify !== nothing ? """
                        LEFT JOIN (SELECT Symbol, Bin FROM Bins) AS PT
                        ON ET.Symbol = PT.Symbol
                    """ : "")
                    $(tov !== nothing ? """
                        LEFT JOIN (SELECT Date, Symbol, OpenPortion, TovBin FROM OpenPortions) AS PT
                        ON ET.Symbol = PT.Symbol AND ET.Date = PT.Date
                        WHERE TovBin != 0
                    """ : "")
                )
                GROUP BY Date, InputHash $(bin ? ", Bin" : "") $(tov_bin ? ", TovBin" : "")
            ) AS ET
            INNER JOIN (
                SELECT Id, Date,
                       groupArray(RunningFrom)[1] RunningFrom,
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
                       groupArray(UnitTradingAmount)[1] AS UnitTradingAmount,
                       groupArray(UnitTradingAmountPair)[1] AS UnitTradingAmountPair,
                       groupArray(UnitTradingVolume)[1] AS UnitTradingVolume,
                       groupArray(StopCriteria)[1] AS StopCriteria,
                       groupArray(SeparateVirtualPos)[1] AS Sep,
                       groupArray(Latency)[1] AS Latency,
                       groupArray(TradeWindow)[1] AS TradeWindow,
                       groupArray(NTradesPerSignal)[1] AS NTradesPerSignal,
                       count(DISTINCT Symbol) AS NSymbols,
                       if(Capital != 0, Capital*1e8, WeightedCapital*1e4*NSymbols) AS TotalCapital
                FROM (
                    SELECT *,
                           REGEXP_EXTRACT(RunningFrom, '.*V\\d+_\\d+', 0) AS s0,
                           REGEXP_EXTRACT(RunningFrom, '.*V\\d+', 0) AS s1,
                           greatest(s0, s1) Strategy,
                           splitByChar('_', RunningFrom)[-1] PredLabel
                    FROM $(input_tb) AS IT
                    INNER JOIN (SELECT Symbol, Date FROM POOL) AS PT
                    ON IT.Symbol = PT.Symbol AND IT.Date = PT.Date
                    WHERE Id IN ids AND Date IN dates
                )
                GROUP BY Id, Date
            ) AS IT
            ON ET.InputHash = IT.Id AND ET.Date = IT.Date
        ) AS ET
    ) AS BT
    $(with_real ? """
    LEFT JOIN (
        SELECT Instrument Symbol, Date,
               sum(Position) NetPositionReal,
               sum(Poundage) FeeReal,
               sum(PnlToday + PnlYestoday - Poundage) T0PnlReal,
               sum(TurnOver) TurnoverReal
        FROM (
            SELECT * FROM datahouse.ProfitTableDistinctView
            WHERE Account LIKE '%E2%' AND Account NOT LIKE 'Dummy_%' AND Account NOT LIKE '%_DC' AND Account NOT LIKE '%_Adj' AND Account NOT LIKE '%_Scale' AND Date IN dates
        ) AS A
        GROUP BY Symbol, Date
    ) AS RT
    ON BT.Symbol = RT.Symbol AND BT.Date = RT.Date
    """ : "")
    SETTINGS max_memory_usage = 800000000000
    """
    # query_by_dt = """
    # SELECT
    # $(with_real ? "" : "--") sum(NetPositionReal * EodPrice) AS NetPositionValueReal,
    # $(with_real ? "" : "--") sum(T0PnlReal) AS T0PnlReal,
    # $(with_real ? "" : "--") sum(TurnoverReal) AS TurnoverReal,
    # --sum(BuyTurnover) AS BuyTurnover,
    # --sum(SellTurnover) AS SellTurnover,
    # --$(with_close ? "" : "--") if(NetBuyOpenValue = 0, 0, sum(SellCloseValue) / NetBuyOpenValue) AS BuyClosedPortion,
    # --$(with_close ? "" : "--") if(NetSellOpenValue = 0, 0, sum(BuyCloseValue) / NetSellOpenValue) AS SellClosedPortion,
    # --sum(LmpOrderAmount) / (sum(LmpOrderAmount) + sum(FakOrderAmount)) AS QuotePortion,
    # $(with_real ? "" : "--") sum(FeeReal) AS FeeReal,
    # FROM ($(query_results)) AS RT
    # GROUP BY Date, InputHash $(bin ? ", Bin" : "") $(tov_bin ? ", TovBin" : "")
    # """

    query = """
    SELECT InputHash,
           $(bin ? "" : "--") Bin,
           $(tov_bin ? "" : "--") TovBin,
           groupArray(RunningFrom)[1] RunningFrom,
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
           groupArray(Latency)[1] AS Latency,
           groupArray(TradeWindow)[1] AS TradeWindow,
           groupArray(NTradesPerSignal)[1] AS NTradesPerSignal,
           -- groupArray(Sep)[1] Sep,
           avg(NSymbols) AS NSymbols,
           avg(TotalCapital) AS TotalCapital,
           avg(NetPositionValue) AS NetPositionValue,
           avg(PositionValue) AS PositionValue,
           $(with_real ? "" : "--") avg(NetPositionValueReal) AS NetPositionValueReal,
           avg(T0Pnl) AS T0Pnl,
           $(with_real ? "" : "--") avg(T0PnlReal) AS T0PnlReal,
           avg(OvernightPnl) AS OvernightPnl,
           avg(IntradayPnl) AS IntradayPnl,
           --avg(BuyOpenTradePnl) AS BuyOpenTradePnl,
           --avg(SellOpenTradePnl) AS SellOpenTradePnl,
           --avg(BuyCloseTradePnl) AS BuyCloseTradePnl,
           --avg(SellCloseTradePnl) AS SellCloseTradePnl,
           --avg(BuyForceCloseTradePnl) AS BuyForceCloseTradePnl,
           --avg(SellForceCloseTradePnl) AS SellForceCloseTradePnl,
           --avg(FillRate) AS FillRate,
           avg(Turnover) AS Turnover,
           --avg(BuyTurnover) AS BuyTurnover,
           --avg(SellTurnover) AS SellTurnover,
           --avg(BuyOpenTurnover) AS BuyOpenTurnover,
           --avg(SellOpenTurnover) AS SellOpenTurnover,
           --avg(BuyCloseTurnover) AS BuyCloseTurnover,
           --avg(SellCloseTurnover) AS SellCloseTurnover,
           --avg(BuyForceCloseTurnover) AS BuyForceCloseTurnover,
           --avg(SellForceCloseTurnover) AS SellForceCloseTurnover,
           --$(with_close ? "" : "--") avg(BuyClosedPortion) AS BuyClosedPortion,
           --$(with_close ? "" : "--") avg(SellClosedPortion) AS SellClosedPortion,
           --avg(BuyOpenCount) AS BuyOpenCount,
           --avg(SellOpenCount) AS SellOpenCount,
           --avg(QuotePortion) QuotePortion,
           $(with_real ? "" : "--") avg(TurnoverReal) TurnoverReal,
           $(with_real ? "" : "--") avg(FeeReal) FeeReal,
           avg(Fee) Fee
    FROM ($(query_by_dt))
    GROUP BY InputHash $(bin ? ", Bin" : "") $(tov_bin ? ", TovBin" : "")
    ORDER BY Capital, Offset
    """

    comp_date = select_df(conn, query_by_dt)
    if nrow(comp_date) == 0
        return nothing
    end

    sort!(comp_date, :Date)

    comp_avg = select_df(conn, query)

    if parse_theo
        comp_avg[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_avg.TheoModel, "_")]
        comp_date[!, :TheoModel] = [x[2][1:end-1] for x in split.(comp_date.TheoModel, "_")]
    end

    cols = [:InputHash, :RunningFrom,
        # :OvnPriceFactor, :OvnQtyFactor,
        :Portfolio, :Capital, :WeightedCapital, :NSymbols, :TotalCapital, :TheoModel, :Latency, :TradeWindow, :NTradesPerSignal, :PLF,
        :Offset, :BuyOffset, :SellOffset, :OffsetRange, :SkipFirstMinutes,
        :Skewness, :UnitTradingAmount, :UnitTradingAmountPair, :UnitTradingVolume, :StopCriteria, :BiasFactor,
        :Strategy,
        :PredLabel,
        :T0Pnl, :OvernightPnl, :IntradayPnl,
        # :BuyOpenTradePnl, :SellOpenTradePnl, :BuyCloseTradePnl, :SellCloseTradePnl, :BuyForceCloseTradePnl, :SellForceCloseTradePnl,
        :PositionValue,
        :NetPositionValue,
        # :FillRate, :QuotePortion,
        :Turnover,
        # :BuyTurnover, :SellTurnover, :BuyOpenTurnover, :SellOpenTurnover, :BuyCloseTurnover, :SellCloseTurnover, :BuyForceCloseTurnover, :SellForceCloseTurnover, :BuyOpenCount, :SellOpenCount,
        :Fee,
    ]

    if with_close
        append!(cols, [:BuyClosedPortion, :SellClosedPortion])
    end

    if with_real
        append!(cols, [:T0PnlReal, :NetPositionValueReal, :TurnoverReal, :FeeReal])
    end

    if bin
        push!(cols, :Bin)
    end

    if tov_bin
        push!(cols, :TovBin)
    end

    comp_avg = comp_avg[!, cols]
    @rtransform!(comp_avg, :TurnoverRatio = :Turnover / (:TotalCapital * 2))
    @rtransform!(comp_avg, :NetPositionValueRatio = :NetPositionValue / :TotalCapital)
    @rtransform!(comp_avg, :PositionValueRatio = :PositionValue / :TotalCapital)
    @rtransform!(comp_avg, :T0Rtn = :T0Pnl / :TotalCapital * 260)
    @rtransform!(comp_avg, :T0RtnOfTov = :T0Pnl / :Turnover * 260)
    @rtransform!(comp_avg, :OvernightRtn = :OvernightPnl / :TotalCapital * 260)
    @rtransform!(comp_avg, :OvernightRtnOfTov = :OvernightPnl / :Turnover * 260)
    @rtransform!(comp_avg, :IntradayRtn = :IntradayPnl / :TotalCapital * 260)
    @rtransform!(comp_avg, :IntradayRtnOfTov = :IntradayPnl / :Turnover * 260)

    @rtransform!(comp_date, :TurnoverRatio = :Turnover / (:TotalCapital * 2))
    @rtransform!(comp_date, :NetPositionValueRatio = :NetPositionValue / :TotalCapital)
    @rtransform!(comp_date, :PositionValueRatio = :PositionValue / :TotalCapital)
    @rtransform!(comp_date, :T0Rtn = :T0Pnl / :TotalCapital * 260)
    @rtransform!(comp_date, :T0RtnOfTov = :T0Pnl / :Turnover * 260)
    @rtransform!(comp_date, :OvernightRtn = :OvernightPnl / :TotalCapital * 260)
    @rtransform!(comp_date, :OvernightRtnOfTov = :OvernightPnl / :Turnover * 260)
    @rtransform!(comp_date, :IntradayRtn = :IntradayPnl / :TotalCapital * 260)
    @rtransform!(comp_date, :IntradayRtnOfTov = :IntradayPnl / :Turnover * 260)

    # comp_avg[!, :TradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    # if with_close
    # comp_avg[!, :BuyTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    # comp_avg[!, :SellTradePnl] = comp_avg.SellOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl;
    # else
    # comp_avg[!, :BuyTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.BuyCloseTradePnl .+ comp_avg.BuyForceCloseTradePnl;
    # comp_avg[!, :SellTradePnl] = comp_avg.SellOpenTradePnl .+ comp_avg.SellCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    # end
    # comp_avg[!, :OpenTradePnl] = comp_avg.BuyOpenTradePnl .+ comp_avg.SellOpenTradePnl;
    # comp_avg[!, :CloseTradePnl] = comp_avg.BuyCloseTradePnl .+ comp_avg.SellCloseTradePnl;
    # comp_avg[!, :ForceCloseTradePnl] = comp_avg.BuyForceCloseTradePnl .+ comp_avg.SellForceCloseTradePnl;
    # comp_avg[!, :BuyTurnoverRatio] = comp_avg.BuyTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :SellTurnoverRatio] = comp_avg.SellTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :BuyOpenTurnoverRatio] = comp_avg.BuyOpenTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :SellOpenTurnoverRatio] = comp_avg.SellOpenTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :BuyCloseTurnoverRatio] = comp_avg.BuyCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :SellCloseTurnoverRatio] = comp_avg.SellCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :BuyForceCloseTurnoverRatio] = comp_avg.BuyForceCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :SellForceCloseTurnoverRatio] = comp_avg.SellForceCloseTurnover ./ (comp_avg.TotalCapital .* 2);
    # comp_avg[!, :BuySellTurnoverRatioDiff] = comp_avg.BuyTurnoverRatio .- comp_avg.SellTurnoverRatio;
    # comp_avg[!, :IntradayInventoryRtn] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :IntradayInventoryRtnOfTov] = (comp_avg.IntradayPnl .- comp_avg.TradePnl) ./ (comp_avg.Turnover) .* 260;
    # comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover) .* 260;
    # comp_avg[!, :TradeRtn] = comp_avg.TradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :TradeRtnOfTov] = comp_avg.TradePnl ./ (comp_avg.Turnover) .* 260;
    # comp_avg[!, :BuyTradeRtn] = comp_avg.BuyTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :BuyTradeRtnOfTov] = comp_avg.BuyTradePnl ./ (comp_avg.BuyOpenTurnover .+ comp_avg.SellCloseTurnover .+ comp_avg.SellForceCloseTurnover) .* 260;
    # comp_avg[!, :SellTradeRtn] = comp_avg.SellTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :SellTradeRtnOfTov] = comp_avg.SellTradePnl ./ (comp_avg.SellOpenTurnover .+ comp_avg.BuyCloseTurnover .+ comp_avg.BuyForceCloseTurnover) .* 260;
    # comp_avg[!, :BuyOpenTradeRtn] = comp_avg.BuyOpenTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :BuyOpenTradeRtnOfTov] = comp_avg.BuyOpenTradePnl ./ (comp_avg.BuyOpenTurnover) .* 260;
    # comp_avg[!, :SellOpenTradeRtn] = comp_avg.SellOpenTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :SellOpenTradeRtnOfTov] = comp_avg.SellOpenTradePnl ./ (comp_avg.SellOpenTurnover) .* 260;
    # comp_avg[!, :BuyCloseTradeRtn] = comp_avg.BuyCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :BuyCloseTradeRtnOfTov] = comp_avg.BuyCloseTradePnl ./ (comp_avg.BuyCloseTurnover) .* 260;
    # comp_avg[!, :SellCloseTradeRtn] = comp_avg.SellCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :SellCloseTradeRtnOfTov] = comp_avg.SellCloseTradePnl ./ (comp_avg.SellCloseTurnover) .* 260;
    # comp_avg[!, :BuyForceCloseTradeRtn] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :BuyForceCloseTradeRtnOfTov] = comp_avg.BuyForceCloseTradePnl ./ (comp_avg.BuyForceCloseTurnover) .* 260;
    # comp_avg[!, :SellForceCloseTradeRtn] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.TotalCapital) .* 260;
    # comp_avg[!, :SellForceCloseTradeRtnOfTov] = comp_avg.SellForceCloseTradePnl ./ (comp_avg.SellForceCloseTurnover) .* 260;

    # comp_date[!, :TradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.BuyForceCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    # if with_close
    # comp_date[!, :BuyTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    # comp_date[!, :SellTradePnl] = comp_date.SellOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.BuyForceCloseTradePnl;
    # else
    # comp_date[!, :BuyTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.BuyCloseTradePnl .+ comp_date.BuyForceCloseTradePnl;
    # comp_date[!, :SellTradePnl] = comp_date.SellOpenTradePnl .+ comp_date.SellCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    # end
    # comp_date[!, :OpenTradePnl] = comp_date.BuyOpenTradePnl .+ comp_date.SellOpenTradePnl;
    # comp_date[!, :CloseTradePnl] = comp_date.BuyCloseTradePnl .+ comp_date.SellCloseTradePnl;
    # comp_date[!, :ForceCloseTradePnl] = comp_date.BuyForceCloseTradePnl .+ comp_date.SellForceCloseTradePnl;
    # comp_date[!, :BuyOpenTurnoverRatio] = comp_date.BuyOpenTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :SellOpenTurnoverRatio] = comp_date.SellOpenTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :BuyCloseTurnoverRatio] = comp_date.BuyCloseTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :SellCloseTurnoverRatio] = comp_date.SellCloseTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :BuyForceCloseTurnoverRatio] = comp_date.BuyForceCloseTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :SellForceCloseTurnoverRatio] = comp_date.SellForceCloseTurnover ./ (comp_date.TotalCapital .* 2);
    # comp_date[!, :IntradayInventoryRtn] = (comp_date.IntradayPnl .- comp_date.TradePnl) ./ (comp_date.TotalCapital);
    # comp_date[!, :IntradayInventoryRtnOfTov] = (comp_date.IntradayPnl .- comp_date.TradePnl) ./ (comp_date.Turnover);
    # comp_date[!, :TradeRtn] = comp_date.TradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :TradeRtnOfTov] = comp_date.TradePnl ./ (comp_date.Turnover);
    # comp_date[!, :BuyTradeRtn] = comp_date.BuyTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :SellTradeRtn] = comp_date.SellTradePnl ./ (comp_date.TotalCapital);
    # if with_close
    # comp_date[!, :BuyTradeRtnOfTov] = comp_date.BuyTradePnl ./ (comp_date.BuyOpenTurnover .+ comp_date.SellCloseTurnover .+ comp_date.SellForceCloseTurnover);
    # comp_date[!, :SellTradeRtnOfTov] = comp_date.SellTradePnl ./ (comp_date.SellOpenTurnover .+ comp_date.BuyCloseTurnover .+ comp_date.BuyForceCloseTurnover);
    # else
    # comp_date[!, :BuyTradeRtnOfTov] = comp_date.BuyTradePnl ./ (comp_date.BuyOpenTurnover .+ comp_date.BuyCloseTurnover .+ comp_date.BuyForceCloseTurnover);
    # comp_date[!, :SellTradeRtnOfTov] = comp_date.SellTradePnl ./ (comp_date.SellOpenTurnover .+ comp_date.SellCloseTurnover .+ comp_date.SellForceCloseTurnover);
    # end
    # comp_date[!, :BuyOpenTradeRtn] = comp_date.BuyOpenTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :BuyOpenTradeRtnOfTov] = comp_date.BuyOpenTradePnl ./ (comp_date.BuyOpenTurnover);
    # comp_date[!, :SellOpenTradeRtn] = comp_date.SellOpenTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :SellOpenTradeRtnOfTov] = comp_date.SellOpenTradePnl ./ (comp_date.SellOpenTurnover);
    # comp_date[!, :BuyCloseTradeRtn] = comp_date.BuyCloseTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :BuyCloseTradeRtnOfTov] = comp_date.BuyCloseTradePnl ./ (comp_date.BuyCloseTurnover);
    # comp_date[!, :SellCloseTradeRtn] = comp_date.SellCloseTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :SellCloseTradeRtnOfTov] = comp_date.SellCloseTradePnl ./ (comp_date.SellCloseTurnover);
    # comp_date[!, :BuyForceCloseTradeRtn] = comp_date.BuyForceCloseTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :BuyForceCloseTradeRtnOfTov] = comp_date.BuyForceCloseTradePnl ./ (comp_date.BuyForceCloseTurnover);
    # comp_date[!, :SellForceCloseTradeRtn] = comp_date.SellForceCloseTradePnl ./ (comp_date.TotalCapital);
    # comp_date[!, :SellForceCloseTradeRtnOfTov] = comp_date.SellForceCloseTradePnl ./ (comp_date.SellForceCloseTurnover);
    # DfUtils.fillna!(comp_date.BuyTradeRtnOfTov, 0)
    # DfUtils.fillna!(comp_date.SellTradeRtnOfTov, 0)
    # DfUtils.fillna!(comp_date.BuyCloseTradeRtnOfTov, 0)
    # DfUtils.fillna!(comp_date.SellCloseTradeRtnOfTov, 0)
    # DfUtils.fillna!(comp_date.BuyForceCloseTradeRtnOfTov, 0)
    # DfUtils.fillna!(comp_date.SellForceCloseTradeRtnOfTov, 0)

    return comp_date, comp_avg, to_classify !== nothing ? round.(bins, digits=3) : nothing
end

function get_trade_rtn(conn, date, ih, capital)
    select_df(
        conn,
        """
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
"""
    )
end

function get_trade_rtn(conn, ih, capital)
    select_df(
        conn,
        """
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
"""
    )
end

end
