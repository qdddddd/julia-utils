module CommonUtils
export to_datetime, sample, count_values, squeeze

using Dates
using StatsBase

function to_datetime(ts::Int64, precision::Int = 6)::DateTime
    epoch = 621355968000000000
    if (ts > epoch)
        return unix2datetime((ts - epoch) / 10^7)
    else
        return unix2datetime(ts / 10^precision + 8 * 3600)
    end
end

function sample(set, ratio = 0.1)
    len = size(set)[1]
    rands = floor.(Int, rand(floor(Int, len * ratio)) .* len .+ 1)
    return set[rands, :]
end

function count_values(arr, rev::Bool = true)
    u_arr = unique(arr)
    counts = [val => count(==(val), arr) for val in u_arr]
    return sort(counts, by = x -> x.second, rev = rev)
end

squeeze(arr) = dropdims(arr, dims = tuple(findall(size(arr) .== 1)...))

end
