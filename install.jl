using Pkg, Logging

const JLUTILS_GIT_URL = get(ENV, "JLUTILS_GIT_URL", "https://github.com/qdddddd/julia-utils.git")
const JLUTILS_GIT_REV = get(ENV, "JLUTILS_GIT_REV", "master")
const PYTHON_BIN = something(Sys.which("python3"), Sys.which("python"), nothing)
const MAX_GIT_RETRIES = parse(Int, get(ENV, "JLUTILS_GIT_RETRIES", "3"))

function install_from_git!()
    last_err = nothing
    for attempt in 1:MAX_GIT_RETRIES
        try
            Pkg.add(url=JLUTILS_GIT_URL, rev=JLUTILS_GIT_REV)
            Pkg.build()
            Pkg.precompile()
            Pkg.status(["JlUtils"])
            return
        catch err
            last_err = err
            @warn "JlUtils git install attempt failed" attempt=attempt retries=MAX_GIT_RETRIES exception=(err, catch_backtrace())
            attempt == MAX_GIT_RETRIES || sleep(5 * attempt)
        end
    end

    throw(last_err)
end

if PYTHON_BIN === nothing
    error("python3/python not found in PATH")
end

@info "Installing Python dependencies" python=PYTHON_BIN
ENV["PYTHON"] = PYTHON_BIN
run(`$PYTHON_BIN -m pip install -U pip`)
run(`$PYTHON_BIN -m pip install pymssql`)

@info "Installing JlUtils from git" url=JLUTILS_GIT_URL rev=JLUTILS_GIT_REV retries=MAX_GIT_RETRIES
ENV["JULIA_PKG_USE_CLI_GIT"] = "true"
Pkg.activate()
install_from_git!()

