.onLoad <- function(libname, pkgname)
{
    setHDF5DumpDir()
    setHDF5DumpCompressionLevel()
    file.create(get_HDF5_dump_logfile())
}
