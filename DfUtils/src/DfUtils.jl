module DfUtils
export read_hdf, head, tail, ffill, backfill, fillna, fillna!, interpolate, from_hdf, shift, collapse, parallel_apply, parallel_apply!, add_bins!, round_df!, rolling_sum, rolling_mean

using DataFrames, DataFramesMeta, Base.Threads, StatsBase, HDF5, FileIO
using CommonUtils: to_datetime, squeeze, format_number

function read_hdf(filename::AbstractString)
    return load(filename)
end

head(df, n=5) = df[1:min(nrow(df), n), :]
tail(df, n=5) = df[max(end - n + 1, 1):end, :]

_eq(a, b) = ifelse(ismissing(b), ismissing(a), a == b)
ffill(v, mark=missing) = v[[ifelse(x != 0, x, 1) for x in accumulate(max, .!_eq.(v, mark) .* (1:length(v)))], :]
backfill(v, mark=missing) = reverse(ffill(reverse(v), mark))
fillna(v::AbstractVecOrMat, val) = map(x -> isnan(x) ? val : x, v)
fillna!(v::AbstractVecOrMat, val) = map!(x -> isnan(x) ? val : x, v, v)
fillna(df::DataFrame, val) = map(col -> fillna(col, val), eachcol(df))

function interpolate(df::DataFrame, ts::AbstractVector, column::Symbol, ts_column::Symbol=:Timestamp; rev=false)
    l_tmp = DataFrame(ts_column => ts, column => missing)
    l_tmp[!, :label] .= !rev

    r_tmp = df[!, [ts_column, column]]
    r_tmp[!, :label] .= rev

    joined = sort(vcat(l_tmp, r_tmp), [ts_column, :label], rev=rev)
    joined[!, column] .= ffill(joined[!, column])

    return joined[joined.label.==!rev, column]
end

function from_hdf(
    filename::AbstractString;
    feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing,
    window_feature_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing,
    label_names::Union{Vector{AbstractString},Vector{Symbol},Nothing}=nothing,
    insert_ts::Bool=true
)
    X1, X2, Y = nothing, nothing, nothing

    h5open(filename, "r") do file
        hdf = read(file)

        if (!haskey(hdf, "x"))
            return X1, X2, Y
        end

        ts = insert_ts ? hdf["timestamp"] .|> to_datetime : nothing
        seq = haskey(hdf, "appseq") ? hdf["appseq"] : missing

        if !isnothing(feature_names)
            X1 = DataFrame(hdf["x"] |> squeeze |> transpose, feature_names)
            insert_ts && insertcols!(X1, 1, :Timestamp => ts)
        end

        if haskey(hdf, "wx") && !isnothing(window_feature_names)
            X2 = DataFrame(hdf["wx"] |> squeeze |> transpose, window_feature_names)
            insert_ts && insertcols!(X2, 1, :Timestamp => ts)
        end

        if !isnothing(label_names)
            Y = DataFrame(transpose(hdf["y"]), label_names)
            insert_ts && insertcols!(Y, 1, :Timestamp => ts)
        end

        if !ismissing(seq)
            !isnothing(X1) && insertcols!(X1, 1, :AppSeq => seq)
            !isnothing(X2) && insertcols!(X2, 1, :AppSeq => seq)
            !isnothing(Y) && insertcols!(Y, 1, :AppSeq => seq)
        end
    end

    return X1, X2, Y
end

function shift(arr::AbstractVector, n::Int, fill_missing=missing)
    ret = Any[x for x in arr]

    if n > 0
        ret[1:n] .= fill_missing
        ret[n+1:end] .= @view arr[1:end-n]
    elseif n < 0
        ret[end+n+1:end] .= fill_missing
        ret[1:end+n] .= @view arr[-n+1:end]
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
    bins = Int.(collect(1:floor(nrow(df) / n):nrow(df)))
    if bins[end] != nrow(df)
        push!(bins, nrow(df))
    end
    partitions = [@view df[bins[i]:bins[i+1], :] for i in 1:length(bins)-1]

    @threads for part in partitions
        func(part)
    end
    df
end

function parallel_apply(df::DataFrame, func, n=10)
    bins = Int.(collect(1:floor(nrow(df) / n):nrow(df)))
    if bins[end] != nrow(df)
        push!(bins, nrow(df))
    end
    partitions = [@view df[bins[i]:bins[i+1], :] for i in 1:length(bins)-1]
    tasks = []

    @sync for part in partitions
        push!(tasks, @spawn func(part))
    end

    reduce(vcat, fetch.(tasks))
end

function add_bins!(df, col; n_bins=10, bin_col_name=:Bin)
    df[!, bin_col_name] .= 0
    bins = nquantile(df[!, col], n_bins)

    for i in 1:length(bins)-1
        lower = bins[i]
        upper = bins[i+1]

        if i == 1
            df[df[!, col].<upper, bin_col_name] .= i
        elseif i == length(bins) - 1
            df[df[!, col].>=lower, bin_col_name] .= i
        else
            df[df[!, col].>=lower.&&df[!, col].<upper, bin_col_name] .= i
        end
    end
    df
end

function round_df!(df; digits=0)
    df[!, (<:).(eltype.(eachcol(df)), Union{Float64,Float32,Missing})] .= round.(df[!, (<:).(eltype.(eachcol(df)), Union{Float64,Float32,Missing})], digits=digits)
    df
end

function rolling_sum(a, n::Int; forward=false, fill=false)
    len = length(a)
    out = Vector{Union{Missing,eltype(a)}}(missing, len)

    if n > len && !fill
        return out
    end

    if forward
        out[1] = sum(a[1:min(n, len)])

        @views for i in 2:len
            if i + n - 1 <= len
                out[i] = out[i-1] + a[i+n-1] - a[i-1]
            elseif fill
                out[i] = out[i-1] - a[i-1]
            end
        end
    else
        out[1] = a[1]
        @views for i in 2:len
            if i <= n
                out[i] = out[i-1] + a[i]
            else
                out[i] = out[i-1] + a[i] - a[i-n]
            end
        end

        if !fill
            out[1:min(n, len)-1] .= missing
        end
    end
    out
end

function rolling_mean(a, n::Int; forward=false)
    len = length(a)
    out = Vector{Float64}(undef, len)

    if forward
        c = min(n, len)
        s = sum(@view a[1:c])

        out[1] = s / c

        @views for i in 2:len
            if i + n - 1 <= len
                s = s + a[i+n-1] - a[i-1]
            else
                s = s - a[i-1]
                c -= 1
            end

            out[i] = s / c
        end
    else
        c = 0
        s = 0
        @views for i in 1:len
            if i <= n
                c += 1
                s += a[i]
            else
                s = s + a[i] - a[i-n]
            end

            out[i] = s / c
        end
    end
    out
end

end
