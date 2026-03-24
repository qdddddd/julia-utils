using Pkg, Logging

const JLUTILS_GIT_URL = get(ENV, "JLUTILS_GIT_URL", "https://github.com/qdddddd/julia-utils.git")
const JLUTILS_GIT_REV = get(ENV, "JLUTILS_GIT_REV", "master")
const PYTHON_BIN = something(Sys.which("python3"), Sys.which("python"), nothing)

if PYTHON_BIN === nothing
    error("python3/python not found in PATH")
end

@info "Installing Python dependencies" python=PYTHON_BIN
ENV["PYTHON"] = PYTHON_BIN
run(`$PYTHON_BIN -m pip install -U pip`)
run(`$PYTHON_BIN -m pip install pymssql`)

@info "Installing JlUtils from git" url=JLUTILS_GIT_URL rev=JLUTILS_GIT_REV
Pkg.activate()
try
    Pkg.rm("JlUtils")
catch err
    @info "JlUtils was not already installed in the active environment" exception=(err, catch_backtrace())
end
Pkg.add(url=JLUTILS_GIT_URL, rev=JLUTILS_GIT_REV)
Pkg.build()
Pkg.precompile()
Pkg.status(["JlUtils"])

