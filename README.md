
# Renjin HDF5 Package

Work in progress. Currently only supports reading 64-bit floating
point chunked arrays.

This is a pure-Java implementation based on the 
[HDF5 File Format Specification Version 3.0](https://support.hdfgroup.org/HDF5/doc/H5.format.html).
 

## Using from Renjin

The goal of this package is to allow access to large HDF objects
from R/Renjin without having to load the whole object into memory.

The API is inspired by the [HDF5Array](http://bioconductor.org/packages/release/bioc/html/HDF5Array.html)
package.

    library("org.renjin:hdf5")
    da <- HDF5Array("/my/data.h5", 'zscore_psiSite')
    
    dim(da)
    typeof(da)  # normal matrix, no special classes
    patient3 <- da[3,]


    