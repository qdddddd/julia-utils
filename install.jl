using Pkg

print("Install deps globally (y/n): ")
if readline()[1] == 'y'
    deps = []
    to_rm = []
    for name in readdir(@__DIR__)
        if name == "false"
            continue
        end
        dir = joinpath(@__DIR__, name)
        if isdir(dir) && !startswith(name, ".")
            push!(to_rm, name)
            # Read Project.toml line by line and extract lines after "[deps]"
            to_read = false
            for line in eachline(joinpath(dir, "Project.toml"))
                if startswith(line, "[deps]")
                    to_read = true
                    continue
                end

                if !to_read
                    continue
                end

                push!(deps, split(line, " =")[1])
            end
        end
    end
    unique!(deps)
    filter!(x -> x ∉ to_rm, deps)

    for dep in deps
        Pkg.add(dep)
    end
end

run(`pip3 install pymssql`)

for name in readdir(@__DIR__)
    if name == "false"
        continue
    end
    dir = joinpath(@__DIR__, name)
    if isdir(dir) && !startswith(name, ".")
        Pkg.activate(name)
        Pkg.update()
        Pkg.activate()
        Pkg.develop(path=dir)
    end
end

Pkg.build()
Pkg.precompile()
