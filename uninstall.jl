using Pkg

for name in readdir(@__DIR__)
    if name == "false"
        continue
    end
    dir = joinpath(@__DIR__, name)
    if isdir(dir) && !startswith(name, ".")
        Pkg.rm(name)
    end
end
