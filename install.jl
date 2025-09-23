using Pkg, Logging

@info "Installing Python dependencies"
ENV["PYTHON"] = Sys.which("python3")
run(`pip3 install -U pip`)
run(`pip3 install pymssql`)
Pkg.activate("JlUtils")
Pkg.update()
Pkg.activate()
Pkg.develop(path=@__DIR__)
Pkg.build()
Pkg.precompile()
