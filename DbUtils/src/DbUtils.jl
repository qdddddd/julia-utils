module DbUtils

export connect_ch, reconnect, conn, execute_queries, get_md, get_index, get_od, get_td, mc_readdir, mc_isfile, mc_ispath, get_future_months, get_next_trading_day_if_holiday, get_next_trading_day, get_prev_trading_day, get_future_md, query_mssql

using CSV, ClickHouse, Minio, XMLDict, JSON, DataFrames, DataFramesMeta, Dates, Logging
using CommonUtils

include("../../constants.jl")

ch_conf = parse_xml(read(joinpath(homedir(), ".clickhouse-client/config.xml"), String))
_minio_cfg = JSON.parsefile(joinpath(homedir(), ".mc/config.json"))["aliases"]["remote"]
minio_cfg = MinioConfig(_minio_cfg["url"]; username=_minio_cfg["accessKey"], password=_minio_cfg["secretKey"])

function connect_ch()
    conn = ClickHouse.connect(ch_conf["host"], 9000; username=ch_conf["user"], password=ch_conf["password"])
    ClickHouse.execute(conn, "SET max_memory_usage = 1280000000000")
    conn
end

function reconnect()
    global _conn = connect_ch()
    _conn
end

_conn = connect_ch()
conn() = is_connected(_conn) ? _conn : reconnect()

function execute_queries(queries, to_throw=false)
    for q in queries
        try
            execute(_conn, q)
        catch e
            if to_throw
                throw(e)
            end
            reconnect()
        end
    end
end

function get_md(symbol, date, nan=true)
    dt = CommonUtils.format_dt(date)
    postfix = dt < "20230223" ? "" : "-lc"
    bucket = (endswith(symbol, "SZ") ? "sze" : "sse") * postfix
    fn = "$(dt)/$(symbol)_$(dt).gz"

    if !mc_isfile("$(bucket)/$(fn)") return nothing end

    df = CSV.read(
        s3_get(minio_cfg, bucket, fn), DataFrame;
        select=[:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover],
        types=Dict(:BidPrice1 => Float64, :AskPrice1 => Float64, :ExTime => DateTime),
        dateformat="yyyy-mm-dd HH:MM:SS.s",
        ntasks=1
    )

    if nan
        replace!(df[!, :BidPrice1], 0 => NaN)
        replace!(df[!, :AskPrice1], 0 => NaN)
    end
    # unique!(df, :ExTime)
    sort!(df, :ExTime)
    return df
end

function get_index(name, date)
    dt = CommonUtils.format_dt(date)
    bucket = "ivocapmarket"
    fn = "$(dt)/$(index_codes[name]).csv.gz"

    if !mc_isfile("$(bucket)/$(fn)")
        @warn "Missing index data $(name) on $(date)"
        return
    end

    df = CSV.read(s3_get(minio_cfg, bucket, fn), DataFrame; select=[:TimeInMillSeconds, :LastPrice])
    df = combine(groupby(df, :TimeInMillSeconds), last)
    df[!, :ExTime] = unix2datetime.(df.TimeInMillSeconds ./ 1000 .+ (8*3600))
    sort!(df, :TimeInMillSeconds)
    return df[!, [:ExTime, :LastPrice]]
end

_date_fmt = dateformat"yyyy-mm-dd HH:MM:SS.s"

function get_od(symbol, date)
    postfix = date < "20230223" ? "" : "-lc"
    bucket = (endswith(symbol, "SZ") ? "szeorder" : "sseorder") * postfix
    df = CSV.read(s3_get(minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz"), DataFrame)

    if endswith(symbol, "SZ")
        df[!, :Timestamp] = DateTime.(df.Timestamp, _date_fmt)

        if hasproperty(df, :ExTime)
            df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        end
    else
        if hasproperty(df, :ExTime)
            df[!, :Timestamp] = DateTime.(df.Timestamp, _date_fmt)
            df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        else
            df[!, :Timestamp] = CommonUtils.to_datetime.(df.Timestamp, 3)
        end
    end

    sort!(df, :Timestamp)
    return df
end

function get_td(symbol, date)
    postfix = ""

    if date > "20230223"
        postfix = "-lc"
    end

    bucket = (endswith(symbol, "SZ") ? "szetrade" : "ssetrade") * postfix
    df = CSV.read(s3_get(minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz"), DataFrame)

    if hasproperty(df, :Timestamp)
        time_col = :Timestamp
    else
        time_col = :TimeStamp
    end

    # if endswith(symbol, "SZ")
        # df[!, time_col] = DateTime.(df[!, time_col], _date_fmt)

        # if hasproperty(df, :ExTime)
            # df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        # end
    # else
        # if hasproperty(df, :ExTime)
            # df[!, time_col] = DateTime.(df[!, time_col], _date_fmt)
            # df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        # else
            # df[!, time_col] = CommonUtils.to_datetime.(df[!, time_col], 3)
        # end
    # end

    sort!(df, time_col)
    return df
end

function mc_readdir(path)
    if path[end] != "/"
        path = path * "/"
    end

    readdir(S3Path("s3://$(path)", config=minio_cfg))
end

mc_isfile(fn) = isfile(S3Path("s3://$(fn)", config=minio_cfg))
mc_ispath(path) = ispath(S3Path("s3://$(path)", config=minio_cfg))

struct IndexMonthCode
    YearOffset::Int
    Month::String
end

function get_future_months(date::Date)
    curMonth = month(date)
    curYear = year(date)

    if day(date) >= 15
        firstDay = DateTime(curYear, curMonth, 1)
        firstDayToFri = (Friday - dayofweek(firstDay)) % 7
        thirdFri = firstDay + Day(firstDayToFri >= 0 ? 14 : 21)

        deliveryDate = Date(thirdFri)
        while is_holiday(deliveryDate)
            deliveryDate = get_next_trading_day(deliveryDate)
        end

        if day(date) > day(deliveryDate)
            if curMonth == 12
                curYear += 1
            end
            curMonth = month(firstDay + Month(1))
        end
    end

    activeMonths = [
        [IndexMonthCode(0, "01"), IndexMonthCode(0, "02"), IndexMonthCode(0, "03"), IndexMonthCode(0, "06")],
        [IndexMonthCode(0, "02"), IndexMonthCode(0, "03"), IndexMonthCode(0, "06"), IndexMonthCode(0, "09")],
        [IndexMonthCode(0, "03"), IndexMonthCode(0, "04"), IndexMonthCode(0, "06"), IndexMonthCode(0, "09")],
        [IndexMonthCode(0, "04"), IndexMonthCode(0, "05"), IndexMonthCode(0, "06"), IndexMonthCode(0, "09")],
        [IndexMonthCode(0, "05"), IndexMonthCode(0, "06"), IndexMonthCode(0, "09"), IndexMonthCode(0, "12")],
        [IndexMonthCode(0, "06"), IndexMonthCode(0, "07"), IndexMonthCode(0, "09"), IndexMonthCode(0, "12")],
        [IndexMonthCode(0, "07"), IndexMonthCode(0, "08"), IndexMonthCode(0, "09"), IndexMonthCode(0, "12")],
        [IndexMonthCode(0, "08"), IndexMonthCode(0, "09"), IndexMonthCode(0, "12"), IndexMonthCode(1, "03")],
        [IndexMonthCode(0, "09"), IndexMonthCode(0, "10"), IndexMonthCode(0, "12"), IndexMonthCode(1, "03")],
        [IndexMonthCode(0, "10"), IndexMonthCode(0, "11"), IndexMonthCode(0, "12"), IndexMonthCode(1, "03")],
        [IndexMonthCode(0, "11"), IndexMonthCode(0, "12"), IndexMonthCode(1, "03"), IndexMonthCode(1, "06")],
        [IndexMonthCode(0, "12"), IndexMonthCode(1, "01"), IndexMonthCode(1, "03"), IndexMonthCode(1, "06")]
    ]

    ret = [string(curYear + x.YearOffset % 100, x.Month)[3:end] for x in activeMonths[curMonth]]

    return ret
end

is_holiday(date::Date) = date in holidays

function get_next_trading_day_if_holiday(date::Date)
    day_of_week = dayofweek(date)

    if day_of_week == Saturday
        return get_next_trading_day_if_holiday(date + Day(2))
    elseif day_of_week == Sunday
        return get_next_trading_day_if_holiday(date + Day(1))
    else
        return is_holiday(date) ? get_next_trading_day_if_holiday(date + Day(1)) : date
    end
end

function get_next_trading_day(date::Date)
    return get_next_trading_day_if_holiday(date + Day(1))
end

function get_prev_trading_day(date::Date)
    day_of_week = Dates.dayofweek(date)

    if day_of_week == Dates.Monday
        return get_prev_trading_day(date - Dates.Day(2))
    elseif day_of_week == Dates.Sunday
        return get_prev_trading_day(date - Dates.Day(1))
    else
        prev = date - Dates.Day(1)
        return is_holiday(prev) ? get_prev_trading_day(prev) : prev
    end
end

function get_future_md(symbol, date, nan=true)
    dt = CommonUtils.format_dt(date)
    bucket = "option"
    fn = "$(dt)/$(symbol)_$(dt).gz"

    if !mc_isfile("$(bucket)/$(fn)") return nothing end

    df = CSV.read(
        s3_get(minio_cfg, bucket, fn), DataFrame;
        select=[:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover],
        types=Dict(:BidPrice1 => Float64, :AskPrice1 => Float64, :ExTime => DateTime),
        dateformat="yyyy-mm-dd HH:MM:SS.s",
        ntasks=1
    )

    if nan
        replace!(df[!, :BidPrice1], 0 => NaN)
        replace!(df[!, :AskPrice1], 0 => NaN)
    end
    # unique!(df, :ExTime)
    sort!(df, :ExTime)
    return df
end

# mssql
using PyCall
const pymssql = PyNULL()

function __init__()
    copy!(pymssql, pyimport("pymssql"))
end

function query_mssql(query::String)
    sql_conn = pymssql.connect(host="192.168.50.122", port=1433, user="sa", password="1Volution")
    cursor = sql_conn[:cursor](as_dict=true)
    cursor[:execute](query)
    rows = cursor[:fetchall]()
    cursor[:close]()
    sql_conn[:close]()

    DataFrame(Dict(col => [r[col] for r in rows] for col in keys(rows[1])))
end

function query_mssql(queries::Vector{String})
    sql_conn = pymssql.connect(host="192.168.50.122", port=1433, user="sa", password="1Volution")
    res = []

    sql_conn = pymssql.connect(host="192.168.50.122", port=1433, user="sa", password="1Volution")

    for q in queries
        cursor = sql_conn[:cursor](as_dict=true)
        cursor[:execute](q)
        rows = cursor[:fetchall]()
        push!(res, DataFrame(Dict(col => [r[col] for r in rows] for col in keys(rows[1]))))
        cursor[:close]()
    end

    sql_conn[:close]()
    res
end

end
