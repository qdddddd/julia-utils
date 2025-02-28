module DfUtils
export read_hdf, head, tail, ffill, backfill, fillna, fillna!, interpolate, from_hdf, shift, collapse, parallel_apply, parallel_apply!, add_bins!, round_df!, rolling_sum, rolling_mean, rolling_sum_prod, rolling_min, rolling_max, skipna, fixna

using DataFrames, DataFramesMeta, Base.Threads, StatsBase, HDF5, FileIO
using ..CommonUtils: to_datetime, squeeze, format_number
using Base: @propagate_inbounds

function read_hdf(filename::AbstractString)
    return load(filename)
end

head(df, n=5) = df[1:min(nrow(df), n), :]
tail(df, n=5) = df[max(end - n + 1, 1):end, :]

_eq(a, b) = ifelse(ismissing(b), ismissing(a), a == b)
ffill(v, mark=missing) = v[[ifelse(x != 0, x, 1) for x in accumulate(max, .!_eq.(v, mark) .* (1:length(v)))]]
backfill(v, mark=missing) = reverse(ffill(reverse(v), mark))
fillna(v::AbstractVecOrMat, val) = map(x -> isnan(x) || ismissing(x) ? val : x, v)
fillna!(v::AbstractVecOrMat, val) = map!(x -> isnan(x) || ismissing(x) ? val : x, v, v)
fillna(df::DataFrame, val) = map(col -> fillna(col, val), eachcol(df))
function DfUtils.fillna!(df::DataFrame, vals::Pair...)
    map(e -> fillna!(df[!, e.first], e.second), vals)
    disallowmissing!(df)
end

function interpolate(df::Union{SubDataFrame,DataFrame}, ts::AbstractVector, column::Symbol, ts_column::Symbol=:Timestamp; rev=false)
    l_tmp = DataFrame(ts_column => ts, column => missing)
    l_tmp[!, :label] .= !rev

    r_tmp = df[:, [ts_column, column]]
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

    h5open(filename, "r") do hdf
        if (!haskey(hdf, "x"))
            return X1, X2, Y
        end

        ts = insert_ts ? read(hdf, "timestamp") .|> to_datetime : nothing
        seq = haskey(hdf, "appseq") ? read(hdf, "appseq") : missing

        if !isnothing(feature_names)
            X1 = DataFrame(read(hdf, "x") |> squeeze |> transpose, feature_names)
            insert_ts && insertcols!(X1, 1, :Timestamp => ts)
        end

        if haskey(hdf, "wx") && !isnothing(window_feature_names)
            X2 = DataFrame(read(hdf, "wx") |> squeeze |> transpose, window_feature_names)
            insert_ts && insertcols!(X2, 1, :Timestamp => ts)
        end

        if !isnothing(label_names)
            Y = DataFrame(transpose(read(hdf, "y")), label_names)
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

function shift(arr::Union{AbstractVecOrMat,DataFrame}, n::Int, fill_missing=missing)
    ret = ismissing(fill_missing) ? allowmissing(arr) : similar(arr)

    if n > 0
        ret[1:n, :] .= fill_missing
        ret[n+1:end, :] .= @view arr[1:end-n, :]
    elseif n < 0
        ret[end+n+1:end, :] .= fill_missing
        ret[1:end+n, :] .= @view arr[-n+1:end, :]
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

function round_df!(df; digits=0, exclude=nothing)
    isnothing(exclude) && (exclude = String[]) || (exclude = string.(exclude))
    cols = (<:).(eltype.(eachcol(df)), Union{Float64,Float32,Missing}) .& (∉).(names(df), Ref(exclude))
    df[!, cols] .= round.(df[!, cols], digits=digits)
    df
end

function max_skipna(v::AbstractVecOrMat)
    s = zero(eltype(v))
    @inbounds @simd for x in v
        if !ismissing(x) && !isnan(x)
            s = max(s, x)
        end
    end
    s
end

function sum_skipna(v::AbstractVecOrMat)
    s = zero(eltype(v))
    @inbounds @simd for x in v
        if !ismissing(x) && !isnan(x)
            s += x
        end
    end
    s
end

function mean_skipna(v::AbstractVecOrMat)
    s = zero(eltype(v))
    n = 0
    @inbounds @simd for x in v
        if !ismissing(x) && !isnan(x)
            s += x
            n += 1
        end
    end
    s / n
end

fixna(v::Real, mark=0.0) = ismissing(v) || isnan(v) ? eltype(v)(mark) : v

function rolling_sum(a::AbstractVecOrMat{<:Real}, n::Int; forward=false, fill=false)
    len = size(a, 1)
    out = Base.fill(eltype(a)(NaN), size(a))

    if forward
        initrows = eachcol(a[1:min(n, len), :])
        c = count.(!isnan, initrows)
        s = sum_skipna.(initrows)

        out[1, :] = s

        @views for i in 2:len
            oldval = a[i-1, :]
            mask = .!ismissing.(oldval) .&& .!isnan.(oldval)
            s[mask] .-= oldval[mask]
            c[mask] .-= 1

            if i + n - 1 <= len
                newval = a[i+n-1, :]
                mask = .!ismissing.(newval) .&& .!isnan.(newval)
                s[mask] .= fixna.(s[mask]) .+ newval[mask]
                c[mask] .+= 1

            elseif !fill
                break
            end

            s[c.==0] .= eltype(a)(NaN)
            out[i, :] = s
        end
    else
        s = Base.fill(zero(eltype(a)), size(a, 2))
        c = Base.fill(0, size(a, 2))

        @views for i in 1:len
            newval = a[i, :]
            mask = .!ismissing.(newval) .&& .!isnan.(newval)
            s[mask] .= fixna.(s[mask]) .+ newval[mask]
            c[mask] .+= 1

            if i > n
                oldval = a[i-n, :]
                mask = .!ismissing.(oldval) .&& .!isnan.(oldval)
                s[mask] .-= oldval[mask]
                c[mask] .-= 1
            end

            s[c.==0] .= eltype(a)(NaN)

            if !fill && i < n
                continue
            end

            out[i, :] = s
        end
    end
    out
end

function rolling_sum_prod(v1, v2, n::Int; forward=false, fill=false)
    len = length(v1)
    @assert len == length(v2)
    out = Vector{Union{Missing,eltype(v1)}}(missing, len)

    if n > len && !fill
        return out
    end

    if forward
        out[1] = sum_skipna(v1[1:min(n, len)] .* v2[1:min(n, len)])

        @views for i in 2:len
            if i + n - 1 <= len
                out[i] = out[i-1] + fixna(v1[i+n-1] * v2[i+n-1]) - fixna(v1[i-1] * v2[i-1])
            elseif fill
                out[i] = out[i-1] - fixna(v1[i-1] * v2[i-1])
            end
        end
    else
        out[1] = fixna(v1[1] * v2[1])
        @views for i in 2:len
            out[i] = out[i-1] + fixna(v1[i] * v2[i])

            if i > n
                out[i] -= fixna(v1[i-n] * v2[i-n])
            end
        end

        if !fill
            out[1:min(n, len)-1] .= missing
        end
    end
    out
end

function rolling_mean(a::AbstractVecOrMat{<:Real}, n::Int; forward=false)
    len = size(a, 1)
    out = fill(eltype(a)(NaN), size(a))

    if forward
        initrows = eachcol(a[1:min(n, len), :])
        c = count.(!isnan, initrows)
        s = sum_skipna.(initrows)

        out[1, :] = s ./ c

        @views for i in 2:len
            oldval = a[i-1, :]
            mask = .!ismissing.(oldval) .&& .!isnan.(oldval)
            s[mask] .-= oldval[mask]
            c[mask] .-= 1

            if i + n - 1 <= len
                newval = a[i+n-1, :]
                mask = .!ismissing.(newval) .&& .!isnan.(newval)
                s[mask] .+= newval[mask]
                c[mask] .+= 1
            end

            s[c.==0] .= 0
            out[i, :] = s ./ c
        end
    else
        s = fill(zero(eltype(a)), size(a, 2))
        c = fill(0, size(a, 2))

        @views for i in 1:len
            mask = .!ismissing.(a[i, :]) .&& .!isnan.(a[i, :])
            c[mask] .+= 1
            s[mask] .+= a[i, mask]

            if i > n
                mask = .!ismissing.(a[i-n, :]) .&& .!isnan.(a[i-n, :])
                c[mask] .-= 1
                s[mask] .-= a[i-n, mask]
            end

            s[c.==0] .= 0
            out[i, :] = s ./ c
        end
    end
    out
end

function rolling_min(a::AbstractVector{<:Real}, n::Int)
    len = length(a)

    result = Vector{eltype(a)}(undef, len)
    deque = Int[]

    @views for i in 1:len
        if !isempty(deque) && deque[1] <= i - n
            popfirst!(deque)
        end

        while !isempty(deque) && fixna(a[i], Inf) <= fixna(a[deque[end]], Inf)
            pop!(deque)
        end

        push!(deque, i)

        result[i] = a[deque[1]]
    end

    return result
end

function rolling_max(a::AbstractVector{<:Real}, n::Int)
    len = length(a)

    result = Vector{eltype(a)}(undef, len)
    deque = Int[]

    @views for i in 1:len
        if !isempty(deque) && deque[1] <= i - n
            popfirst!(deque)
        end

        while !isempty(deque) && fixna(a[i], -Inf) >= fixna(a[deque[end]], -Inf)
            pop!(deque)
        end

        push!(deque, i)

        result[i] = a[deque[1]]
    end

    return result
end

skipna(itr) = Iterators.filter(x -> !ismissing(x) && !isnan(x), itr)

end
