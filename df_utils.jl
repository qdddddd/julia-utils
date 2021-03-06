module DfUtils
export head, tail, ffill, backfill, interpolate, from_hdf

using DataFrames
include("io.jl")
import .IO.read_hdf
include("common.jl")
import .CommonUtils.to_datetime
import .CommonUtils.squeeze

head(df) = df[1:5, :]
tail(df) = df[end-4:end, :]

_eq(a, b) = ifelse(b === missing, ismissing(a), a == b)
ffill(v, mark = missing) = v[[ifelse(x != 0, x, 1) for x in accumulate(max, .!_eq.(v, mark) .* (1:length(v)))], :]
backfill(v, mark = missing) = reverse(ffill(reverse(v), mark))

function interpolate(df, ts, column, ts_name::Symbol = :Timestamp)
    l_tmp = DataFrame(ts_name => ts, column => missing)
    l_tmp[!, :label] .= 0

    r_tmp = df[!, [ts_name, column]]
    r_tmp[!, :label] .= 1

    joined = sort(vcat(l_tmp, r_tmp), ts_name)
    joined[!, column] .= ffill(joined[!, column])

    return joined[joined.label.==0, column]
end

function from_hdf(
    filename::AbstractString,
    feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing} = nothing,
    window_feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing} = nothing,
    label_names::Union{Vector{AbstractString},Vector{Symbol},Nothing} = nothing
)
    hdf = read_hdf(filename)
    if (!haskey(hdf, "x"))
        return missing, missing
    end

    ts = hdf["timestamp"] .|> to_datetime

    X1 = hdf["x"]
    X1 = squeeze(X1) |> transpose
    X1 = DataFrame(X1, feature_names)

    X2 = DataFrame()
    if haskey(hdf, "wx")
        X2 = hdf["wx"] |> squeeze |> transpose
        X2 = DataFrame(X2, window_feature_names)
        insertcols!(X2, 1, :Timestamp => ts)
    end

    Y = DataFrame(transpose(hdf["y"]), label_names)
    insertcols!(Y, 1, :Timestamp => ts)

    return X1, X2, Y
end

end
