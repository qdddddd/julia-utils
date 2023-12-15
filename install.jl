using Pkg

Pkg.activate()

run(`pip3 install pymssql`)

for name in readdir(@__DIR__)
    if name == "false"
        continue
    end
    dir = joinpath(@__DIR__, name)
    if isdir(dir) && !startswith(name, ".")
        Pkg.develop(path=dir)
    end
end

Pkg.build()
Pkg.precompile()
