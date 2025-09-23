module Logger
export make_logger

using Logging, LoggingExtras, Dates

function filter_logging_modules(logger)
    EarlyFilteredLogger(logger) do log
        log._module === Main && parentmodule(log._module) === Main
    end
end

function tsfmt(level, _module, group, id, file, line)
    color, prefix, _ = Logging.default_metafmt(level, _module, group, id, file, line)
    prefix = "$(Dates.format(now(), dateformat"yyyy-mm-dd HH:MM:SS.sss")) $prefix"
    color, prefix, ""
end

make_logger(loglevel) = ConsoleLogger(stdout, loglevel; meta_formatter=tsfmt) |> filter_logging_modules |> global_logger

end
