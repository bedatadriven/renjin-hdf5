
library("org.renjin:hdf")

d <- '/home/alex/Downloads'
f <- 'zscore_psiSite.h5'
fp <- file.path(d,f)
da <- HDF5Array(fp, sub('\\.h5','',f))

print(dim(da))
