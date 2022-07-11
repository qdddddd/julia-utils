module IO
export read_hdf

using HDF5

_lk = ReentrantLock()
function read_hdf(filename::AbstractString, lk = _lk)
    lock(lk)
    hdf = h5open(filename, "r") do file
        read(file)
    end
    unlock(lk)

    return hdf
end

end
