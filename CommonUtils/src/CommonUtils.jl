module CommonUtils
using Base: AbstractVecOrTuple
export to_datetime, sample, count_values, squeeze, format_number, join_str, format_dt, colwise, rowwise, product, get_fee_rate, compile, to_milli, to_readable_size, totalsize

using Dates, StatsBase, Statistics, .Threads

include("precompiler.jl")

function to_datetime(ts::Int64, precision::Int=6)::DateTime
    epoch = 621355968000000000
    if (ts > epoch)
        return unix2datetime((ts - epoch) / 10^7)
    else
        return unix2datetime(ts / 10^precision + 8 * 3600)
    end
end

function sample(set, ratio=0.1)
    len = size(set)[1]
    rands = floor.(Int, rand(floor(Int, len * ratio)) .* len .+ 1)
    return set[rands, :]
end

function count_values(arr, rev::Bool=true)
    u_arr = unique(arr)
    counts = [val => count(==(val), arr) for val in u_arr]
    return sort(counts, by=x -> x.second, rev=rev)
end

squeeze(arr) = dropdims(arr, dims=tuple(findall(size(arr) .== 1)...))

function format_number(num)
    if (isa(num, AbstractVecOrTuple))
        num = _format_number.(num)
        return "[" * join(num, ", ") * "]"
    end

    return _format_number(num)
end

function _format_number(num)
    if isa(num, AbstractString)
        return num
    end

    str = split(string(num), '.')
    ret = replace(str[1], r"(?<=[0-9])(?=(?:[0-9]{3})+(?![0-9]))" => ",")
    if length(str) == 2
        return str[2] == "0" ? ret : ret * "." * str[2]
    else
        return ret
    end
end

import Base.*
*(a::Symbol, b::Symbol) = Symbol(string(a) * string(b))
*(a::Symbol, b::String) = Symbol(string(a) * b)

join_str(vec) = """'$(join(vec, "','"))'"""

function format_dt(dt, fmt=dateformat"yyyymmdd")
    if isa(dt, AbstractString)
        return dt
    end

    if isa(dt, Date) || isa(dt, DateTime)
        return Dates.format(dt, fmt)
    end

    if !isa(dt, AbstractVecOrTuple)
        return string(dt)
    end

    if isa(dt, Number)
        return string(dt)
    end

    uqv = unique(dt)
    uqvDate = Dates.format.(uqv, fmt)
    dateDict = Dict(uqv .=> uqvDate)
    map(x -> dateDict[x], dt)
end

rowwise(f, A) = [f(view(A, i, :)) for i in 1:size(A, 1)]
colwise(f, A) = [f(view(A, :, j)) for j in 1:size(A, 2)]

product(x::AbstractArray, y::AbstractArray) = vec(Iterators.product(x, y) |> collect)

get_fee_rate(date::String) = date < "20230828" ? 0.00065 : 0.00035
get_fee_rate(date::Date) = date < Date(2023, 8, 28) ? 0.00065 : 0.00035

function to_milli(str)
    if str[end] == 's'
        return parse(Int, str[1:end-1]) * 1000
    elseif str[end] == 'm'
        return parse(Int, str[1:end-1]) * 60 * 1000
    elseif str[end] == 'h'
        return parse(Int, str[1:end-1]) * 3600 * 1000
    elseif str[end] == 'd'
        return parse(Int, str[1:end-1]) * 24 * 3600 * 1000
    else
        return parse(Int, str)
    end
end

function to_readable_size(bytes::Number)
    sizes = ["B", "KB", "MB", "GB", "TB"]
    i = 1
    while bytes >= 1024
        if i == length(sizes)
            break
        end

        bytes /= 1024
        i += 1
    end

    string(round(bytes, digits=2), " ", sizes[i])
end

function totalsize(dirpath; noprint=false)
    ret = Atomic{Int64}(0)
    for (root, _, files) in walkdir(dirpath)
        s = filesize(root)
        atomic_add!(ret, s)
        @threads for f in files
            file = joinpath(root, f)
            size = filesize(file)
            atomic_add!(ret, size)
        end
    end

    if !noprint
        print(ret[] / (1024^3), " GB", "\r")
    end

    ret[]
end

end
