module DfUtils
export head, tail, ffill, backfill, interpolate, from_hdf, shift, fillna, fillna!, collapse, parallel_apply, parallel_apply!, add_bins!

using DataFrames, DataFramesMeta, Base.Threads, StatsBase
include("io.jl")
import .IO.read_hdf
include("common.jl")
import .CommonUtils.to_datetime
import .CommonUtils.squeeze
import .CommonUtils.format_number


head(df, n=5) = df[1:n, :]
tail(df, n=5) = df[end-n+1:end, :]

_eq(a, b) = ifelse(b === missing, ismissing(a), a == b)
ffill(v, mark=missing) = v[[ifelse(x != 0, x, 1) for x in accumulate(max, .!_eq.(v, mark) .* (1:length(v)))], :]
backfill(v, mark=missing) = reverse(ffill(reverse(v), mark))
fillna(v::AbstractVecOrMat, val) = map(x -> isnan(x) ? val : x, v)
fillna!(v::AbstractVecOrMat, val) = map!(x -> isnan(x) ? val : x, v, v)
fillna(df::DataFrame, val) = map(col -> fillna(col, val), eachcol(df))

function interpolate(df::DataFrame, ts::AbstractVector, column::Symbol, ts_column::Symbol=:Timestamp)
    l_tmp = DataFrame(ts_column => ts, column => missing)
    l_tmp[!, :label] .= 1

    r_tmp = df[!, [ts_column, column]]
    r_tmp[!, :label] .= 0

    joined = sort(vcat(l_tmp, r_tmp), [ts_column, :label])
    joined[!, column] .= ffill(joined[!, column])

    return joined[joined.label.==1, column]
end

function from_hdf(
    filename::AbstractString,
    feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing,
    window_feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing,
    label_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing
)
    hdf = read_hdf(filename)
    if (!haskey(hdf, "x"))
        return missing, missing
    end

    ts = hdf["timestamp"] .|> to_datetime
    seq = haskey(hdf, "appseq") ? hdf["appseq"] : missing

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

    if seq !== missing
        insertcols!(X1, 1, :AppSeq => seq)
        insertcols!(X2, 1, :AppSeq => seq)
        insertcols!(Y, 1, :AppSeq => seq)
    end

    return X1, X2, Y
end

function shift(arr::AbstractVector, n::Int, fill_missing=missing)
    ret = Any[x for x in arr]

    if n > 0
        ret[1:n] .= fill_missing
        ret[n+1:end] = arr[1:end-n]
    elseif n < 0
        ret[end+n+1:end] .= fill_missing
        ret[1:end+n] = arr[-n+1:end]
    end

    return ret
end

function collapse(df, rows::AbstractVector{Symbol}, col::Symbol, stat::Symbol)
    cols = vcat(rows, [col], [stat])
    combine(groupby(df[:, cols], rows)) do g
        row_ind = g[1, rows]
        to_perm = DataFrame(col => format_number.(g[!, col]), stat => g[!, stat])
        ret = permutedims(to_perm, 1)
        select!(ret, Not(col))
        ret = round.(ret, digits=3)
        for i = eachindex(rows)
            ret[!, rows[i]] .= [row_ind[i]]
        end
        ret
    end
end

function collapse(df, col::Symbol, stats::AbstractVector{Symbol})
    rets = []
    for stat in stats
        to_perm = DataFrame(col => format_number.(df[!, col]), stat => df[!, stat])
        ret = permutedims(to_perm, 1)
        select!(ret, Not(col))
        insertcols!(ret, 1, :Metric => stat)
        push!(rets, ret)
    end
    vcat(rets...)
end

function parallel_apply!(df::DataFrame, func, n=10)
    bins = Int.(collect(1:floor(nrow(df)/n):nrow(df)))
    if bins[end] != nrow(df)
        push!(bins, nrow(df))
    end
    partitions = [@view df[bins[i]:bins[i+1], :] for i in 1:length(bins)-1]

    @threads for part in partitions
        func(part)
    end
    df
end;

function parallel_apply(df::DataFrame, func, n=10)
    bins = Int.(collect(1:floor(nrow(df)/n):nrow(df)))
    if bins[end] != nrow(df)
        push!(bins, nrow(df))
    end
    partitions = [@view df[bins[i]:bins[i+1], :] for i in 1:length(bins)-1]
    tasks = []

    @sync for part in partitions
        push!(tasks, @spawn func(part))
    end

    reduce(vcat, fetch.(tasks))
end;

function add_bins!(df, col; n_bins=10, bin_col_name=:Bin)
    df[!, bin_col_name] .= 0
    bins = nquantile(df[!, col], n_bins)

    for i in 1:length(bins)-1
        lower = bins[i]
        upper = bins[i+1]

        if i == 1
            df[df[!, col] .< upper, bin_col_name] .= i
        elseif i == length(bins)-1
            df[df[!, col] .>= lower, bin_col_name] .= i
        else
            df[df[!, col] .>= lower .&& df[!, col] .< upper, bin_col_name] .= i
        end
    end
    df
end;

end
