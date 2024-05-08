module DbUtils

export connect_ch, reconnect, conn, execute_queries, get_md, get_index, get_od, get_td, mc_readdir, mc_isfile, mc_ispath, get_future_months, get_next_trading_day_if_holiday, get_next_trading_day, get_prev_trading_day, get_future_md, query_mssql, select_df, ClickHouseClient

using CSV, ClickHouse, Minio, XMLDict, JSON, DataFrames, DataFramesMeta, Dates, Logging
using CommonUtils: format_dt, to_datetime

include("../../constants.jl")

global _conn = nothing
global ch_conf = nothing
global mini_cfg = nothing

function __init__()
    global ch_conf = parse_xml(read(joinpath(homedir(), ".clickhouse-client/config.xml"), String))
    _minio_cfg = JSON.parsefile(joinpath(homedir(), ".mc/config.json"))["aliases"]["remote"]
    global minio_cfg = MinioConfig(_minio_cfg["url"]; username=_minio_cfg["accessKey"], password=_minio_cfg["secretKey"])

    copy!(pymssql, pyimport("pymssql"))
end

function connect_ch()
    global ch_conf
    conn = ClickHouse.connect(ch_conf["host"], parse(Int, ch_conf["port"]); username=ch_conf["user"], password=ch_conf["password"])
    ClickHouse.execute(conn, "SET max_memory_usage = 1280000000000")
    conn
end

function reconnect()
    global _conn = connect_ch()
    _conn
end

function conn()
    global _conn
    if _conn !== nothing && is_connected(_conn)
        return _conn
    else
        return reconnect()
    end
end

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

# ===========================================================

mutable struct ClickHouseClient
    pool::Vector{ClickHouseSock}
    lks::Vector{ReentrantLock}
    max_connections::Int
    id::Int

    ClickHouseClient(max_connections::Int = 10) = new(
        [connect_ch() for _ in 1:max_connections], [ReentrantLock() for _ in 1:(max_connections+1)], max_connections, 0
    )
end

function get_conn!(client::ClickHouseClient)
    i = lock(client.lks[end]) do
        client.id = client.id % client.max_connections + 1
        client.id
    end

    lock(client.lks[i]) do
        if !is_connected(client.pool[i])
            client.pool[i] = connect_ch()
        end
    end

    return client.pool[i], client.lks[i]
end

function select_df(client::ClickHouseClient, query::String)
    c, lk = get_conn!(client)
    lock(lk) do
        ClickHouse.select_df(c, query)
    end
end

# ===========================================================

function get_md(date, symbol, nan=true)
    dt = format_dt(date)
    bucket = ""
    postfix = ""
    if endswith(symbol, "SZ")
        bucket = "sze"
        postfix = dt < "20160510" ? "" : "-lc"
    else
        bucket = "sse"
        postfix = dt < "20211020" ? "" : "-lc"
    end

    bucket *= postfix
    fn = "$(dt)/$(symbol)_$(dt).gz"

    if !mc_isfile("$(bucket)/$(fn)") return nothing end

    global minio_cfg
    df = CSV.File(s3_get(minio_cfg, bucket, fn);
        select=[:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover],
        types=Dict(:BidPrice1 => Float32, :AskPrice1 => Float32, :ExTime => DateTime),
        dateformat="yyyy-mm-dd HH:MM:SS.s",
        ntasks=1
    ) |> DataFrame

    if nan
        replace!(df[!, :BidPrice1], 0 => NaN)
        replace!(df[!, :AskPrice1], 0 => NaN)
    end
    # unique!(df, :ExTime)
    sort!(df, :ExTime)
    return df
end

function get_index(name, date)
    dt = format_dt(date)
    bucket = "ivocapmarket"
    fn = "$(dt)/$(index_codes[name]).csv.gz"

    if !mc_isfile("$(bucket)/$(fn)")
        return
    end

    global minio_cfg
    df = CSV.File(s3_get(minio_cfg, bucket, fn); select=[:TimeInMillSeconds, :LastPrice]) |> DataFrame
    df = combine(groupby(df, :TimeInMillSeconds), last)
    df[!, :ExTime] = unix2datetime.(df.TimeInMillSeconds ./ 1000 .+ (8*3600))
    sort!(df, :TimeInMillSeconds)
    return df[!, [:ExTime, :LastPrice]]
end

_date_fmt = dateformat"yyyy-mm-dd HH:MM:SS.s"

function get_od(date, symbol)
    bucket = ""
    postfix = ""
    if endswith(symbol, "SZ")
        bucket = "szeorder"
        postfix = dt < "20160510" ? "" : "-lc"
    else
        bucket = "sseorder"
        postfix = dt < "20211020" ? "" : "-lc"
    end

    bucket *= postfix

    global minio_cfg
    df = CSV.File(s3_get(minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz")) |> DataFrame

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
            df[!, :Timestamp] = to_datetime.(df.Timestamp, 3)
        end
    end

    sort!(df, :Timestamp)
    return df
end

function get_td(date, symbol)
    bucket = ""
    postfix = ""
    if endswith(symbol, "SZ")
        bucket = "szetrade"
        postfix = dt < "20160510" ? "" : "-lc"
    else
        bucket = "ssetrade"
        postfix = dt < "20211020" ? "" : "-lc"
    end

    bucket *= postfix

    global minio_cfg
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

    global minio_cfg
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

get_next_trading_day_if_holiday(date::AbstractString) = get_next_trading_day_if_holiday(Dates.Date(date, "yyyymmdd"))

function get_next_trading_day(date::Date)
    return get_next_trading_day_if_holiday(date + Day(1))
end

get_next_trading_day(date::AbstractString) = get_next_trading_day(Dates.Date(date, "yyyymmdd"))

function get_prev_trading_day_if_holiday(date::Date)
    day_of_week = dayofweek(date)

    if day_of_week == Saturday
        return get_prev_trading_day_if_holiday(date - Day(1))
    elseif day_of_week == Sunday
        return get_prev_trading_day_if_holiday(date - Day(2))
    else
        return is_holiday(date) ? get_prev_trading_day_if_holiday(date - Day(1)) : date
    end
end

get_prev_trading_day_if_holiday(date::AbstractString) = get_prev_trading_day_if_holiday(Dates.Date(date, "yyyymmdd"))

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

get_prev_trading_day(date::AbstractString) = get_prev_trading_day(Dates.Date(date, "yyyymmdd"))

function get_future_md(date, symbol, nan=true)
    dt = format_dt(date)
    bucket = "option"
    fn = "$(dt)/$(symbol)_$(dt).gz"

    if !mc_isfile("$(bucket)/$(fn)") return nothing end

    global minio_cfg
    df = CSV.read(
        s3_get(minio_cfg, bucket, fn), DataFrame;
        select=[:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover],
        types=Dict(:BidPrice1 => Float32, :AskPrice1 => Float32, :ExTime => DateTime),
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
