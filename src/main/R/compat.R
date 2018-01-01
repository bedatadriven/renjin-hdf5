
# Compatibility with HDF5Array Bioconductor Package
# These classes are not used by this implementation,
# but may need to exist in order for dependent packages to build

setClass("HDF5Array")

setClass("HDF5Matrix", contains=c("HDF5Array"))