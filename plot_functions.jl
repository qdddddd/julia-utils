module PlotFunc
export plot_group, plot_curves, plot_pnl, get_bt_trades, plot_trades, plot_bt_trades, plot_table, to_file

using DataFrames, ClickHouse, Dates, ColorSchemes, PlotlyJS

include("common.jl")
include("constants.jl")
include("df_utils.jl")
include("db_utils.jl")

global palette = ColorSchemes.Set2_8

function plot_group(grouped, stat::Symbol, legends, gvars; real=false, showlegend=false, title=nothing, clrs=nothing)
    if clrs !== nothing
        global palette = clrs
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
                marker=attr(color=palette[y[2]]),
                showlegend=showlegend
            )
            for y in zip([string(f) for f in variable], 1:length(variable))
        ]
    else
        bars = [
            bar(
                x=df.x, y=df[!, y[1]],
                name="$(y[1])",
                marker=attr(color=palette[y[2]]),
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
    color_vec = range(gradient[1], gradient[end], length=nrow(tbl) * length(legend_cols))
    header_color = :white

    ret = []
    for g in (split === nothing ? [tbl] : groupby(tbl, split))
        nums = DataFrames.stack(g, Not(gvars))
        perm = sortperm(nums.value)
        nums[!, :value] = color_vec[[tup[1] for tup in sort([(i, perm[i]) for i in 1:length(perm)], by=(x -> x[2]))]]
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
    column_widths=nothing
)
    header_names = header_names === nothing ? names(df) : header_names

    if title !== nothing
        margin_attr = attr(l=30, r=30, b=0, t=30)
    else
        margin_attr = attr(l=0, r=0, b=0, t=0)
    end

    plot(table(
            header=attr(
                values=["<b>$name</b>" for name in header_names],
                fill_color=:white,
                line_color=default_grid_color,
                font=attr(size=13),
                align="center"
            ),
            cells=attr(
                values=[CommonUtils.format_number.(df[!, c]) for c in names(df)],
                line_color=default_grid_color,
                fill_color=fill_color,
                font=attr(size=13),
                align=cell_align,
                height=25
            ),
            columnwidth=column_widths
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

function get_bt_trades(conn, symbol, date, ih)
    if ih === nothing
        return get_bt_trades(symbol, date)
    end

    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $trade_tb
        WHERE Symbol = '$symbol' AND toDate(Timestamp) = '$date' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = select_df(conn, query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function get_bt_trades(conn, start_date::Date, end_date::Date, ih::String)
    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $trade_tb
        WHERE toDate(Timestamp) >= '$start_date' AND toDate(Timestamp) <= '$end_date' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = select_df(conn, query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function plot_trades(conn, symbol, date, ih)
    dt = CommonUtils.format_dt(date)
    md = DbUtils.get_md(symbol, dt)
    relayout(PlotFunc.plot_trades(PlotFunc.get_bt_trades(conn, symbol, dt, ih), md), title_text="$symbol@$dt")
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

function plot_bt_trades(conn, symbol, date, timerange=nothing; ih=nothing)
    plot_trades(get_bt_trades(conn, symbol, date, ih), get_md(symbol, date), timerange=timerange, title="bt trades $symbol on $date")
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

function plot_md(symbol, date)
    plot([
            scatter(DbUtils.get_md(symbol, date), x=:ExTime, y=:BidPrice1, name="bid price",line=attr(color=:seagreen, width=1)),
            scatter(DbUtils.get_md(symbol, date), x=:ExTime, y=:AskPrice1, name="ask price",line=attr(color=:indianred, width=1))
        ],
        Layout(template=plot_template, title_text="$symbol@$(CommonUtils.format_dt(date))")
    )
end

function to_file(_p, fn, dir="/home/qdu/store/bt_results/")
    w = haskey(_p.plot.layout, :width) ? _p.plot.layout.width + 100 : nothing
    h = haskey(_p.plot.layout, :height) ? _p.plot.layout.height : nothing

    path = dir * fn
    mkpath(dirname(path))
    savefig(_p, path; width=w, height=h)
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

end
