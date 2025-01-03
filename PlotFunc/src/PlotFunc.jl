module PlotFunc
export palette, plot_group, plot_curves, plot_table, plot_pnl, get_bt_trades, plot_trades, plot_bt_trades, plot_md, to_file, get_x_domain, get_y_domain, combine_plots

using DataFrames, ClickHouse, Dates, ColorSchemes, PlotlyJS, Colors, PyCall
using CommonUtils, DfUtils, DbUtils
include("../../constants.jl")

palette = nothing
default_color = nothing
default_grid_color = nothing
plot_template = nothing

function __init__()
    global palette = ColorSchemes.Set2_8
    global default_color = ColorSchemes.Blues[6]
    global default_grid_color = "#E5ECF6"
    # global plot_template = PlotlyJS.plot().plot.layout.template
    # plot_template.layout.plot_bgcolor = :white
    # plot_template.layout.xaxis[:gridcolor] = default_grid_color
    # plot_template.layout.xaxis[:zerolinecolor] = default_grid_color
    # plot_template.layout.xaxis[:linecolor] = default_grid_color
    # plot_template.layout.yaxis[:gridcolor] = default_grid_color
    # plot_template.layout.yaxis[:zerolinecolor] = default_grid_color
    # plot_template.layout.yaxis[:linecolor] = default_grid_color
    # plot_template.layout.paper_bgcolor = :white
    # plot_template.layout.width=1460
    # plot_template.layout.height=500
end

function plot_group(grouped, stat::Symbol, legends, gvars; real=false, showlegend=false, title=nothing, clrs=nothing)
    if clrs === nothing
        clrs = palette
    end

    ticklabel = []
    for row in [first(x)[gvars] for x in grouped]
        s = ifelse(length(gvars) == 1, "", "(")
        s *= join(["$(row[v])" for v in gvars], ", ")
        s *= ifelse(length(gvars) == 1, "", ")")

        push!(ticklabel, s)
    end

    df = DataFrame(:x => ticklabel)
    variable = grouped[1][!, length(legends) > 1 ? legends : legends[1]]
    if (length(legends) == 1)
        for f in variable
            df[!, "$f"] = [g[g[!, legends[1]].==f, stat][1] for g in grouped]
        end
    else
        for f in eachrow(variable)
            var = []
            for g in grouped
                cond = [true for _ in 1:nrow(g)]
                for c in legends
                    cond .&= g[!, c] .== f[c]
                end
                push!(var, g[cond, stat][1])
            end

            df[!, """$(join(f, ", "))"""] = var
        end
    end

    if length(legends) == 1
        bars = [
            bar(
                x=df.x, y=df[!, y[1]],
                name="$(y[1])",
                marker=attr(color=clrs[y[2]]),
                showlegend=showlegend
            )
            for y in zip([string(f) for f in variable], 1:length(variable))
        ]
    else
        bars = [
            bar(
                x=df.x, y=df[!, y[1]],
                name="$(y[1])",
                marker=attr(color=clrs[y[2]]),
                showlegend=showlegend
            )
            for y in zip(["""$(join(f, ", "))""" for f in eachrow(variable)], 1:nrow(variable))
        ]
    end

    if real
        df[!, "real"] = [g[!, string(stat)*"Real"][1] for g in grouped]
        push!(bars, bar(x=df.x, y=df.real, name="Real"))
    end

    title = title === nothing ? "$stat" : "$stat $title"

    return plot(
        bars,
        Layout(
            template=plot_template,
            title_text="$title",
            legend_title_text=length(legends) == 1 ? legends : join(legends, ", "))
    )
end

get_label(vals) = join(lpad.(string.(vals), 6, " "), ", ")

function plot_curves(to_plot, grpby, stat, title, clrs; yaxis_range=nothing)
    sort!(to_plot, grpby)
    groups = groupby(to_plot, grpby)
    p = plot(
        Layout(
            template=plot_template,
            width=1350, height=500,
            title_text=title,
            yaxis_title=stat,
            legend_title_text=join(grpby, ", "),
            yaxis_range=yaxis_range
        ))

    c = 1
    for g in groups
        lb = get_label([g[!, l][1] for l in grpby])
        hover = ["$d " for d in g.Date] .* lb .* [", $p" for p in g[!, stat]]
        add_trace!(p,
            scatter(
                x=g.Date, y=g[!, stat],
                name=lb, hoverinfo="text", hovertext=hover,
                line=attr(width=1),
                ocupacy=0.4,
                marker=attr(color=clrs[c]),
                yaxis="y1"
            ))
        c = (c + 1) % 20
        if c == 0
            c += 1
        end
    end

    return p
end

function plot_table(
    df::DataFrame, gvars::AbstractVector{Symbol}, legend::Symbol, stat::Symbol, split::Union{Symbol,Nothing}=nothing;
    width=600, height=300, column_widths=nothing, title_prefix=nothing, title_postfix=nothing)
    tbl = DfUtils.collapse(df, gvars, legend, stat)
    legend_cols = df[!, legend] |> unique

    gradient = ColorSchemes.linear_blue_95_50_c20_n256
    all_vals = DataFrames.stack(tbl, Not(gvars)).value
    minn = minimum(all_vals)
    maxx = maximum(all_vals)
    header_color = :white

    ret = []
    for g in (split === nothing ? [tbl] : groupby(tbl, split))
        nums = DataFrames.stack(g, Not(gvars))
        nums[!, :value] = weighted_color_mean.(1 .- (nums.value .- minn) ./ (maxx - minn), gradient[1], gradient[end])
        color_df = unstack(nums)

        if (split !== nothing)
            split_val = g[1, split]
            g = g[!, Not(split)]
        end
        for c in gvars
            color_df[!, c] .= header_color
        end

        if (split !== nothing)
            color_df = color_df[!, Not(split)]
        end

        header_names = names(g)
        for i in length(header_names)-length(legend_cols)+1:length(header_names)
            header_names[i] = "$(string(legend)[1])=$(header_names[i])"
        end

        title = stat
        if split !== nothing
            title *= " ($split = $split_val)"
        end

        if title_prefix !== nothing
            title = "$(title_prefix) $(title)"
        end

        if title_postfix !== nothing
            title = "$(title) $(title_postfix)"
        end

        push!(ret,
            plot_table(
                g, width=width, height=height,
                header_names=header_names,
                fill_color=[color_df[!, c] for c in names(color_df)],
                cell_align="right",
                title=title,
                column_widths=column_widths
            ))
    end

    if length(ret) == 1
        ret = ret[1]
    end
    ret
end

function plot_table(
    df, legend::Symbol, stats::AbstractVector{Symbol}; width=1000, height=300, column_widths=nothing)
    tbl = DfUtils.collapse(df, legend, stats)
    header_names = names(tbl)
    for i = 2:length(header_names)
        tbl[!, header_names[i]] = round.(tbl[!, header_names[i]], digits=3)
    end

    legend_cols = df[!, legend] |> unique

    gradient = ColorSchemes.linear_blue_95_50_c20_n256
    if (length(legend_cols) == 1)
        color_vec = [gradient[1]]
    else
        color_vec = range(gradient[1], gradient[end], length=length(legend_cols))
    end
    header_color = :white

    color_df = []
    for nums in eachrow(tbl[:, header_names[2:end]])
        perm = sortperm(Vector(nums))
        @show perm
        nums = color_vec[[tup[1] for tup in sort([(i, perm[i]) for i in 1:length(perm)], by=(x -> x[2]))]]
        push!(color_df, nums)
    end
    color_df = DataFrame(hcat(color_df...) |> permutedims, :auto)
    insertcols!(color_df, 1, :metric => header_color)

    for i in length(header_names)-length(legend_cols)+1:length(header_names)
        header_names[i] = "$(string(legend)[1])=$(header_names[i])"
    end

    plot_table(tbl, width=width, height=height, header_names=header_names, column_widths=column_widths)
end

function plot_table(
    df;
    header_names=nothing, title=nothing, width=1000, height=300, fill_color=nothing, cell_align=["left", "right"],
    column_widths=nothing, font_size=12
)
    header_names = header_names === nothing ? names(df) : header_names

    if title !== nothing
        margin_attr = attr(l=30, r=30, b=0, t=30)
    else
        margin_attr = attr(l=0, r=0, b=0, t=0)
    end

    _column_widths = nothing
    if column_widths !== nothing && length(column_widths) < ncols(df)
        _column_widths = copy(column_widths)
        remaining = length(_column_widths) - ncols(df)
        append!(_column_widths, [(1 - sum(column_widths)) / remaining for i in 1:remaining])
    end

    plot(table(
            header=attr(
                values=["<b>$name</b>" for name in header_names],
                fill_color=:white,
                line_color=default_grid_color,
                font=attr(size=font_size),
                align="center"
            ),
            cells=attr(
                values=[CommonUtils.format_number.(df[!, c]) for c in names(df)],
                line_color=default_grid_color,
                fill_color=fill_color,
                font=attr(size=font_size),
                align=cell_align,
                height=25
            ),
            columnwidth=_column_widths
        ), Layout(title_text=title, width=width, height=height, margin=margin_attr))
end

function plot_pnl(grpby, title="", real_static::Bool=false, real_open::Bool=false, static_param::Bool=false)
    # to_plot = comp_date[
    #     ( comp_date.RealStatc .== real_static  .&& comp_date.RealOpen .== real_open .&& comp_date.StaticParam .== static_param
    #       # .&& comp_date.Skewness .== 3
    #       # .&& comp_date.Strategy .!= "Final"
    #     ),
    #     :]
    to_plot = comp_date[:, :]
    sort!(to_plot, :Date)

    p = plot_curves(to_plot, grpby, :T0Pnl, title)
    # add_trace!(p, scatter(x=groups[1].Date, y=groups[1].PnlReal, name="real", line=attr(color=:indianred, width=3), mode="lines+markers"))

    return p
end

function get_bt_trades(cli, date, symbol, ih)
    if ih === nothing
        return get_bt_trades(date, symbol)
    end

    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $trade_tb
        WHERE Symbol = '$symbol' AND toDate(Timestamp) = '$date' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = query_df(cli, query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function get_bt_trades(cli, start_date::Date, end_date::Date, ih::String)
    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $trade_tb
        WHERE toDate(Timestamp) >= '$start_date' AND toDate(Timestamp) <= '$end_date' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = query_df(cli, query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function plot_trades(conn, date, symbol, ih)
    dt = CommonUtils.format_dt(date)
    md = DbUtils.get_md(dt, symbol)
    relayout(plot_trades(get_bt_trades(conn, dt, symbol, ih), md), title_text="$symbol@$dt")
end

function plot_trades(trades, ob; timerange=nothing, title="")
    _trades = trades
    if _trades === nothing
        return nothing
    end
    _ob = ob
    if (timerange !== nothing)
        _trades = _trades[Time.(_trades.Timestamp).>timerange[1].&&Time.(_trades.Timestamp).<=timerange[2], :]
        _ob = _ob[Time.(_ob.ExTime).>timerange[1].&&Time.(_ob.ExTime).<=timerange[2], :]
    end

    _ob = _ob[Time.(_ob.ExTime).>Time(9, 30, 0), :]

    p = plot([
            scatter(x=_ob.ExTime, y=_ob.BidPrice1, name="bid price", line=attr(color=:seagreen, width=1))
            scatter(x=_ob.ExTime, y=_ob.AskPrice1, name="ask price", line=attr(color=:indianred, width=1))
        ], Layout(template=plot_template, title_text=title, height=500)
    )

    if (nrow(_trades) == 0)
        return p
    end
    return _plot_trades(_trades, p)
end

function plot_bt_trades(conn, date, symbol, timerange=nothing; ih=nothing)
    plot_trades(get_bt_trades(conn, date, symbol, ih), get_md(date, symbol), timerange=timerange, title="bt trades $symbol on $date")
end

function _plot_trades(trades, p; y=:Price, buycolor=:seagreen, sellcolor=:indianred, minn=-1, maxx=-1, buylabel="buy", selllabel="sell")
    min_marker = 10
    max_marker = 50
    if (minn == -1)
        minn = minimum(trades.Turnover)
    end
    if (maxx == -1)
        maxx = maximum(trades.Turnover)
    end

    if minn == maxx
        maxx += 1
        min_marker = 25
    end

    trades.markersize = (trades.Turnover .- minn) ./ (maxx - minn) .* (max_marker - min_marker) .+ min_marker
    trades.hover = (Dates.format.(trades.Timestamp, "yyyy-mm-dd HH:MM:SS.sss V ")
                    .*
                    string.(trades.Volume)
                    .*
                    [" @" * string(x) for x in trades.Price])
    buys = trades[trades.Volume.>0, :]
    sells = trades[trades.Volume.<0, :]

    add_trace!(p, scatter(
        x=buys.Timestamp, y=buys[!, y],
        mode="markers",
        name=buylabel,
        hoverinfo="text", hovertext=buys.hover,
        opacity=0.7,
        marker=attr(size=buys.markersize, color=buycolor)))

    add_trace!(p, scatter(
        x=sells.Timestamp, y=sells[!, y],
        mode="markers",
        name=selllabel,
        hoverinfo="text", hovertext=sells.hover,
        opacity=0.7,
        marker=attr(size=sells.markersize, color=sellcolor)))

    return p
end

function plot_md(date, symbol)
    plot([
            scatter(DbUtils.get_md(date, symbol), x=:ExTime, y=:BidPrice1, name="bid price", line=attr(color=:seagreen, width=1)),
            scatter(DbUtils.get_md(date, symbol), x=:ExTime, y=:AskPrice1, name="ask price", line=attr(color=:indianred, width=1))
        ],
        Layout(template=plot_template, title_text="$symbol@$(CommonUtils.format_dt(date))")
    )
end

function convert_html_to_svg(fn)
    str = match(r"Plotly\.newPlot\((.*?)\)", replace(read(fn, String), "\n" => "")).captures[1]
    str = strip(join(split(str, ",")[2:end], ","))[1:end-1]

    py"""
    import re, json
    def func(json_str, _fn):
        import plotly, json
        call_args = json.loads(f'[{json_str}]')
        plotly_json = {'data': call_args[0], 'layout': call_args[1]}
        p = plotly.io.from_json(json.dumps(plotly_json))
        p.write_image(f'{_fn[:-5]}.svg')
    """

    py"func"(str, fn)
    rm(fn)
end

function to_file(_p, fn, dir="/home/qdu/store/bt_results/")
    w = haskey(_p.plot.layout, :width) ? _p.plot.layout.width + 100 : nothing
    h = haskey(_p.plot.layout, :height) ? _p.plot.layout.height : nothing

    path = dir * fn
    mkpath(dirname(path))
    if (endswith(fn, ".svg"))
        out_file = replace(path, ".svg" => ".html")
        savefig(_p, out_file; width=w, height=h)
        convert_html_to_svg(out_file)
    else
        savefig(_p, path; width=w, height=h)
    end
    return path
end

function get_x_domain(i, n, space)
    w = (1 - (n - 1) * space) / n
    s = (i - 1) * (w + space)
    [s, s + w]
end

function get_y_domain(i, n, space, top_margin=0)
    w = (1 - top_margin - (n - 1) * space) / n
    s = 1 - top_margin - (i - 1) * (w + space)
    [s - w, s]
end

function combine_plots(vec; rows=0, cols=0, title="", horizontal_spacing=0.05, vertical_spacing=0.05, width=1350, height=300, showlegend=true, yaxis_range=nothing)
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
        for _ = len+1:r*c
            push!(empty, plot())
        end
    end

    p = make_subplots(
        rows=r, cols=c,
        subplot_titles=reshape([(haskey(s.plot.layout.title, :text) ? s.plot.layout.title[:text] : "") for s in vcat(vec, empty)], (r, c)),
        horizontal_spacing=horizontal_spacing, vertical_spacing=vertical_spacing, shared_yaxes=yaxis_range !== nothing
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

                relayout!(p, width=width, height=r * height, title_text=title, title=attr(x=0.46, xanchor="center"),
                    legend=attr(title_text=ltt), xaxis_title=xt, margin=attr(r=20)
                )

                for i = 1:c:n
                    relayout!(p, Dict(Symbol("yaxis$(ifelse(i==1, "", i))_range") => yaxis_range))
                end

                return p
            end

            n += 1
        end
    end
end

end
