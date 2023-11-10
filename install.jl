using Pkg

Pkg.activate()

for name in readdir(@__DIR__)
    dir = joinpath(@__DIR__, name)
    if isdir(dir) && !startswith(name, ".")
        Pkg.develop(path=dir)
    end
end

Pkg.build()
Pkg.precompile()
