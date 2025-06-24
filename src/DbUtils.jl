module DbUtils

export execute_queries, get_md, get_index, get_od, get_td, mc_readdir, mc_isfile, mc_ispath, get_future_months, get_next_trading_day_if_holiday, get_next_trading_day, get_prev_trading_day, get_future_md, query_mssql, ClickHouseClient, query_df, gcli

using CSV, ClickHouse, Minio, XMLDict, JSON, DataFrames, DataFramesMeta, Dates, Logging
using ..CommonUtils: format_dt, to_datetime
using ..JlUtils: index_codes

_cli = nothing
ch_conf = nothing
minio_cfg = nothing

function __init__()
    DbUtils.ch_conf = parse_xml(read(joinpath(homedir(), ".clickhouse-client/config.xml"), String))
    _minio_cfg = JSON.parsefile(joinpath(homedir(), ".mc/config.json"))["aliases"]["remote"]
    DbUtils.minio_cfg = MinioConfig(_minio_cfg["url"]; username=_minio_cfg["accessKey"], password=_minio_cfg["secretKey"])

    copy!(pymssql, pyimport("pymssql"))
end

function connect_ch()
    conn = ClickHouse.connect(DbUtils.ch_conf["host"], parse(Int, DbUtils.ch_conf["port"]); username=DbUtils.ch_conf["user"], password=DbUtils.ch_conf["password"])
    ClickHouse.execute(conn, "SET max_memory_usage = 1280000000000")
    conn
end

function execute_queries(queries, to_throw=false)
    for q in queries
        try
            c, lk = get_conn!(DbUtils._cli)
            lock(lk) do
                execute(c, q)
            end
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

    ClickHouseClient(max_connections::Int=1) = new(
        [connect_ch() for _ in 1:max_connections], [ReentrantLock() for _ in 1:(max_connections+1)], max_connections, 0
    )
end

function get_conn!(client::ClickHouseClient)
    i = lock(client.lks[end]) do
        client.id = client.id % client.max_connections + 1
        client.id
    end

    lock(client.lks[i]) do
        if !ClickHouse.is_connected(client.pool[i])
            client.pool[i] = connect_ch()
        end
    end

    return client.pool[i], client.lks[i]
end

function query_df(client::ClickHouseClient, query::String)
    c, lk = get_conn!(client)
    lock(lk) do
        n_trial = 0
        while n_trial < client.max_connections
            try
                return ClickHouse.select_df(c, query)
            catch e
                n_trial += 1

                if e isa EOFError
                    c, lk = get_conn!(client)
                elseif n_trial == client.max_connections
                    throw(e)
                end
            end
        end

        throw(ErrorException("Failed to execute query `$query`. Max number of trials reached"))
    end
end

reconnect!(client::ClickHouseClient) =
    for i in eachindex(client.pool)
        client.pool[i] = connect_ch()
    end

is_connected(cli::ClickHouseClient) = all(ClickHouse.is_connected, cli.pool)

function gcli()
    if !isnothing(DbUtils._cli)
        if !is_connected(DbUtils._cli)
            reconnect!(DbUtils._cli)
        end
    else
        DbUtils._cli = ClickHouseClient()
    end

    return DbUtils._cli
end

function getcolumns(query)
    select_lines = split(query, "SELECT")[2] |> x -> split(x, "FROM")[1]
    delim = ",\n"
    if !occursin(delim, select_lines)
        delim = ", "
    end
    Symbol.(replace.(last.(split.(strip.(split(select_lines, delim)), " ")), "`" => ""))
end

query_df(query::String) = query_df(gcli(), query)[!, getcolumns(query)]
# ===========================================================

function get_md(date, symbol; nan=true, lite=true)
    dt = format_dt(date)
    bucket = ""
    postfix = ""
    if endswith(symbol, "SZ")
        bucket = "sze-lc"
    else
        bucket = "sse"
        postfix = dt < "20240522" ? "-lc" : "-v2"
    end

    bucket *= postfix
    fn = "$(dt)/$(symbol)_$(dt).gz"

    if !mc_isfile("$(bucket)/$(fn)")
        return nothing
    end

    df = CSV.read(
        s3_get(DbUtils.minio_cfg, bucket, fn), DataFrame;
        select=(lite ? [:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover] : nothing),
        types=Dict(:BidPrice1 => Float32, :AskPrice1 => Float32, :ExTime => DateTime),
        dateformat="yyyy-mm-dd HH:MM:SS.s",
        ntasks=1
    )

    df[!, :MidPrice] = (df.BidPrice1 .+ df.AskPrice1) ./ 2
    df_sub = @view df[df.BidPrice1.<1e-3, :]
    df_sub[!, :MidPrice] = df_sub.AskPrice1
    df_sub = @view df[df.AskPrice1.<1e-3, :]
    df_sub[!, :MidPrice] = df_sub.BidPrice1

    if nan
        replace!(df[!, :BidPrice1], 0 => NaN)
        replace!(df[!, :AskPrice1], 0 => NaN)
    end
    # unique!(df, :ExTime)
    sort!(df, [:AppSeq, :ExTime])

    if hasproperty(df, :TimeStamp)
        rename!(df, :TimeStamp => :Timestamp)
    end

    return df
end

function get_index(name, date; unix=false)
    dt = format_dt(date)
    bucket = "ivocapmarket"
    fn = "$(dt)/$(index_codes[name]).csv.gz"

    if !mc_isfile("$(bucket)/$(fn)")
        return nothing
    end

    df = CSV.read(s3_get(DbUtils.minio_cfg, bucket, fn), DataFrame; select=[:TimeInMillSeconds, :LastPrice])
    rename!(df, :TimeInMillSeconds => :ExTime)
    df = combine(groupby(df, :ExTime), last)
    df[!, :LastPrice] = round.(df.LastPrice, digits=4)

    if !unix
        df[!, :ExTime] = unix2datetime.(df.ExTime ./ 1000 .+ (8 * 3600))
    end

    sort!(df, :ExTime)
    return df[!, [:ExTime, :LastPrice]]
end

_date_fmt = dateformat"yyyy-mm-dd HH:MM:SS.s"

function get_od(date, symbol)
    bucket = ""
    postfix = ""
    dt = format_dt(date)
    if endswith(symbol, "SZ")
        bucket = "szeorder-lc"
    else
        bucket = "sseorder"
        postfix = dt < "20240522" ? "-lc" : "-v2"
    end

    bucket *= postfix

    df = CSV.read(s3_get(DbUtils.minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz"), DataFrame)

    if hasproperty(df, :TimeStamp)
        rename!(df, :TimeStamp => :Timestamp)
    end

    if endswith(symbol, "SZ")
        df[!, :Timestamp] = DateTime.(df.Timestamp, _date_fmt)

        if hasproperty(df, :ExTime)
            df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        end

        df[!, :OrderType] .= :Insert
    elseif endswith(symbol, "SH")
        if hasproperty(df, :ExTime)
            df[!, :Timestamp] = DateTime.(df.Timestamp, _date_fmt)
            df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
        else
            df[!, :Timestamp] = to_datetime.(df.Timestamp, 3)
        end

        df[!, :OrderType] = Symbol.(df.OrderType)
    end

    if hasproperty(df, :AppSeqNo)
        rename!(df, :AppSeqNo => :AppSeq)
    end

    if hasproperty(df, :BizIndex)
        rename!(df, :BizIndex => :AppSeq)
    end

    df[!, :Direction] .= Symbol.(df.Direction)

    sort!(df, :AppSeq)
    return df
end

function get_td(date, symbol)
    bucket = ""
    postfix = ""
    dt = format_dt(date)
    is_sz = endswith(symbol, "SZ")
    if is_sz
        bucket = "szetrade-lc"
    else
        bucket = "ssetrade"
        postfix = dt < "20240522" ? "-lc" : "-v2"
    end

    bucket *= postfix

    df = CSV.read(s3_get(DbUtils.minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz"), DataFrame)

    if hasproperty(df, :TimeStamp)
        rename!(df, :TimeStamp => :Timestamp)
    end

    if hasproperty(df, :Timestamp)
        df[!, :Timestamp] = DateTime.(df[!, :Timestamp], _date_fmt)
    end

    if hasproperty(df, :ExTime)
        df[!, :ExTime] = DateTime.(df.ExTime, _date_fmt)
    end

    if hasproperty(df, :AppSeqNo)
        rename!(df, :AppSeqNo => :AppSeq)
    end

    if hasproperty(df, :BizIndex)
        rename!(df, :BizIndex => :AppSeq)
    end

    if endswith(symbol, "SZ")
        df[!, :Type] = ifelse.(df.TradeType .== "4", :Cancel, :Completed)
        select!(df, Not(:TradeType))
    elseif endswith(symbol, "SH")
        df[!, :Type] .= :Completed
    end

    sort!(df, :AppSeq)
    return df
end

function mc_readdir(path)
    if path[end] != "/"
        path = path * "/"
    end

    readdir(S3Path("s3://$(path)", config=DbUtils.minio_cfg))
end

mc_isfile(fn) = isfile(S3Path("s3://$(fn)", config=DbUtils.minio_cfg))
mc_ispath(path) = ispath(S3Path("s3://$(path)", config=DbUtils.minio_cfg))

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

    if !mc_isfile("$(bucket)/$(fn)")
        return nothing
    end

    df = CSV.read(
        s3_get(DbUtils.minio_cfg, bucket, fn), DataFrame;
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
