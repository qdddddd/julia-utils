#!/bin/bash
#=
exec julia "${BASH_SOURCE[0]}" "$@"
=#

using PackageCompiler, Glob

function compile(args)
    img_name = "sysimg.so"
    root = pwd()
    if length(args) >= 1
        root = args[1]
    elseif length(args) >= 2
        img_name = args[2]
    end

    build_dir = "$(root)/build"
    img_path = "$(build_dir)/$(img_name)"
    mkpath(build_dir)

    exe_files = Vector{String}()
    packages = Vector{String}()

    # Get packages to compile by
    # parsing the "using" statements
    for (r, _, _) in walkdir(root)
        for f in glob(glob"*.jl", r)
            push!(exe_files, f)
            using_statements = filter(line -> startswith(line, "using "), readlines(open(f)))
            for line in using_statements
                line = filter(c -> !isspace(c), line[7:end])
                append!(packages, split(line, ","))
            end
        end
    end

    unique!(packages)
    @show packages
    @show exe_files
    create_sysimage(packages; sysimage_path=img_path, precompile_execution_file=exe_files, incremental=true)
end

if abspath(PROGRAM_FILE) == @__FILE__
    compile(ARGS)
end
