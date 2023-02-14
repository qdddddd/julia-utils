module CommonUtils
export to_datetime, sample, count_values, squeeze, combine_plots, *

using Dates
using StatsBase
using PlotlyJS

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

function combine_plots(vec; rows=0, cols=0, title="", horizontal_spacing=0.05, vertical_spacing=0.1, width=1350, height=300, showlegend=true)
    if length(vec) == 0
        return
    end

    len = length(vec)

    if rows == 0 && cols == 0
        r = len
        c = 1
    elseif rows == 0
        r = Int(ceil(len / cols))
        c = cols
    elseif cols == 0
        r = rows
        c = Int(ceil(len / rows))
    else
        r = rows
        c = cols
    end

    empty = []
    if (len < r * c)
        for _ = len+1 : r*c
            push!(empty, plot())
        end
    end

    p = make_subplots(
        rows=r, cols=c,
        subplot_titles=reshape([(haskey(s.plot.layout.title, :text) ? s.plot.layout.title[:text] : "") for s in vcat(vec, empty)], (r, c)),
        horizontal_spacing=horizontal_spacing, vertical_spacing=vertical_spacing
    )

    n = 1
    for i in 1:r
        for j in 1:c
            for t in vec[n].plot.data
                restyle!(t, showlegend=(showlegend && n == 1))
                add_trace!(p, t, row=i, col=j)
            end

            if n == length(vec)
                ltt = nothing
                if haskey(vec[1].plot.layout.legend, :title)
                    if haskey(vec[1].plot.layout.legend[:title], :text)
                        ltt = vec[1].plot.layout.legend[:title][:text]
                    end
                end

                xt = nothing
                if haskey(vec[1].plot.layout.xaxis, :title)
                    xt_dict = vec[1].plot.layout.xaxis[:title]
                    if xt_dict !== nothing && haskey(xt_dict, :text)
                        xt = vec[1].plot.layout.xaxis[:title][:text]
                    end
                end

                relayout!(
                    p, width=width, height=r * height, title_text=title, title=attr(x=0.46, xanchor="center"),
                    legend_title_text=ltt, xaxis_title=xt, margin=attr(r=200)
                )
                return p
            end

            n += 1
        end
    end
end

import Base.*
*(a::Symbol, b::Symbol) = Symbol(string(a)*string(b))

join_str(vec) = """'$(join(vec, "','"))'"""

end
