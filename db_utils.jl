module DbUtils

export connect_ch, reconnect, conn, execute_queries, get_md, get_od

using CSV, ClickHouse, Minio, XMLDict, JSON, DataFrames, DataFramesMeta, Dates

include("common.jl")
import .CommonUtils.format_dt

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

function get_md(symbol, date)
    dt = CommonUtils.format_dt(date)
    postfix = dt < "20230223" ? "" : "-lc"
    bucket = (endswith(symbol, "SZ") ? "sze" : "sse") * postfix
    df = CSV.read(
        s3_get(minio_cfg, bucket, "$(dt)/$(symbol)_$(dt).gz"), DataFrame;
        select=[:ExTime, :AppSeq, :BidPrice1, :AskPrice1, :BidVolume1, :AskVolume1, :Turnover]
    )
    df[!, :ExTime] = DateTime.(df.ExTime, "yyyy-mm-dd HH:MM:SS.s")
    replace!(df[!, :BidPrice1], 0 => NaN)
    replace!(df[!, :AskPrice1], 0 => NaN)
    sort!(df, :ExTime)
    return df
end

function get_index(name, date)
    dt = CommonUtils.format_dt(date)
    df = CSV.read(s3_get(minio_cfg, "stock", "$(dt)/$(index_codes[name])_$(dt).gz"), DataFrame; select=[:ExTime, :LastPrice])
    sort!(df, :ExTime)
    return df
end

function get_od(symbol, date)
    postfix = date < "20230223" ? "" : "-lc"
    bucket = (endswith(symbol, "SZ") ? "szeorder" : "sseorder") * postfix
    df = CSV.read(s3_get(minio_cfg, bucket, "$(date)/$(symbol)_$(date).gz"), DataFrame)

    if endswith(symbol, "SZ")
        df[!, :Timestamp] = DateTime.(df.Timestamp, "yyyy-mm-dd HH:MM:SS.ssssss")

        if hasproperty(df, :ExTime)
            df[!, :ExTime] = DateTime.(df.ExTime, "yyyy-mm-dd HH:MM:SS.ssssss")
        end
    else
        if hasproperty(df, :ExTime)
            df[!, :Timestamp] = DateTime.(df.Timestamp, "yyyy-mm-dd HH:MM:SS.ssssss")
            df[!, :ExTime] = DateTime.(df.ExTime, "yyyy-mm-dd HH:MM:SS.ssssss")
        else
            df[!, :Timestamp] = CommonUtils.to_datetime.(df.Timestamp, 3)
        end
    end

    sort!(df, :Timestamp)
    return df
end

end
