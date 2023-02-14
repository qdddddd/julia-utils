module PlotFunc
export plot_group, plot_curves, plot_pnl, get_bt_trades, plot_trades, plot_bt_trades

using DataFrames, ClickHouse, Dates, ColorSchemes, PlotlyJS

include("constants.jl")

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
            df[!, "$(f)"] = [g[g[!, legends[1]] .== f, stat][1] for g in grouped]
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

    title = title === nothing ? "$(stat)" : "$(stat) $(title)"

    return plot(
        bars,
        Layout(
            template=plot_template,
            title_text="$(title)",
            legend_title_text=length(legends) == 1 ? legends : join(legends, ", "))
    )
end;

get_label(vals) = join(lpad.(string.(vals), 6, " "), ", ")

function plot_curves(to_plot, grpby, stat, title, clrs)
    sort!(to_plot, grpby)
    groups = groupby(to_plot, grpby);
    p = plot(
        Layout(
            template=plot_template,
            width=1350, height=500,
            title_text=title,
            yaxis_title=stat,
            legend_title_text=join(grpby, ", ")
        ))

    c = 1
    for g in groups
        lb = get_label([g[!, l][1] for l in grpby])
        hover = ["$(d) " for d in g.Date] .* lb .* [", $(p)" for p in g[!, stat]]
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

function get_bt_trades(symbol, date, ih)
    if ih === nothing
        return get_bt_trades(symbol, date)
    end

    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $(trade_tb)
        WHERE Symbol = '$(symbol)' AND toDate(Timestamp) = '$(date)' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = select_df(connect_ch(), query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function get_bt_trades(start_date::Date, end_date::Date, ih::String)
    query = """
        SELECT *, Price * FilledVolume Turnover, if(Direction = 'B', FilledVolume, -FilledVolume) Volume
        FROM $(trade_tb)
        WHERE toDate(Timestamp) >= '$(start_date)' AND toDate(Timestamp) <= '$(end_date)' AND InputHash LIKE '$(ih)%'
        ORDER BY Timestamp
    """
    ret = select_df(connect_ch(), query)
    ret.Timestamp .= ret.Timestamp .+ Dates.Hour(8)
    return ret
end

function plot_trades(trades, ob; timerange=nothing, title="")
    _trades = trades
    if _trades === nothing
        return nothing
    end
    _ob = ob
    if (timerange !== nothing)
        _trades = _trades[Time.(_trades.Timestamp) .> timerange[1] .&& Time.(_trades.Timestamp) .<= timerange[2], :]
        _ob = _ob[Time.(_ob.ExTime) .> timerange[1] .&& Time.(_ob.ExTime) .<= timerange[2], :]
    end

    _ob = _ob[Time.(_ob.ExTime) .> Time(9, 30, 0), :]

    p = plot([
            scatter(x=_ob.ExTime, y=_ob.BidPrice1, name="bid price", line=attr(color=:seagreen,width=1))
            scatter(x=_ob.ExTime, y=_ob.AskPrice1, name="ask price", line=attr(color=:indianred, width=1))
        ], Layout(template=plot_template, title_text=title, height=500)
    )

    if (nrow(_trades) == 0) return p end
    return _plot_trades(_trades, p)
end;

function plot_bt_trades(symbol, date, timerange=nothing; ih=nothing)
    plot_trades(get_bt_trades(symbol, date, ih), get_md(symbol, date), timerange=timerange, title="bt trades $(symbol) on $(date)")
end;

function _plot_trades(trades, p; y=:Price, buycolor=:seagreen, sellcolor=:indianred, minn=-1, maxx=-1, buylabel="buy", selllabel="sell")
    min_marker = 10
    max_marker = 50
    if (minn == -1) minn = minimum(trades.Turnover) end
    if (maxx == -1) maxx = maximum(trades.Turnover) end
    trades.markersize = (trades.Turnover .- minn) ./ (maxx - minn) .* (max_marker - min_marker) .+ min_marker
    trades.hover = (Dates.format.(trades.Timestamp, "yyyy-mm-dd HH:MM:SS.sss V ")
                    .* string.(trades.Volume)
                    .* [" @" * string(x) for x in trades.Price])
    buys = trades[trades.Volume .> 0, :]
    sells = trades[trades.Volume .< 0, :]

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
;

end
