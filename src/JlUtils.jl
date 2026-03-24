module JlUtils
export Metrics, CommonUtils, DbUtils, DfUtils, SqlFunc, Logger, has_plotfunc, load_plotfunc!

include("constants.jl")
include("Metrics.jl")
include("CommonUtils.jl")
include("DbUtils.jl")
include("DfUtils.jl")
include("SqlFunc.jl")
include("Logger.jl")

has_plotfunc() = isdefined(@__MODULE__, :PlotFunc)

function load_plotfunc!()
    if has_plotfunc()
        return Base.invokelatest(getfield, @__MODULE__, :PlotFunc)
    end

    try
        include(joinpath(@__DIR__, "PlotFunc.jl"))
        return Base.invokelatest(getfield, @__MODULE__, :PlotFunc)
    catch e
        error(
            "Failed to load JlUtils.PlotFunc. Ensure PlotlyJS, Blink assets, and PyCall are configured on this node. Root cause: " *
            sprint(showerror, e)
        )
    end
end

end
