module IO
export read_hdf

using HDF5, FileIO

function read_hdf(filename::AbstractString)
    return load(filename)
end

end
