
library("org.renjin:hdf")


total.time <- system.time({


d <- '/home/alex/Downloads'
f <- 'zscore_psiSite.h5'
fp <- file.path(d,f)
da <- HDF5Array(fp, sub('\\.h5','',f))

#'
#' # Subsetting by rows
#'

#' loading does not scale linearly
log10(nrow(da))

system.time(r <- as.matrix(da[1:1e3,]))


system.time(as.matrix(da[1:1e3,]))
system.time(as.matrix(da[1:1e4,]))
system.time(as.matrix(da[1:1e5,]))

#' random access is slower than by blocks
system.time(as.matrix(da[sample(1:nrow(da), 1e3),]))


#'
#' # Subsetting by columns
#'
ncol(da)
system.time(as.matrix(da[,1]))
system.time(as.matrix(da[,1:10]))
system.time(as.matrix(da[,1:25]))
system.time(as.matrix(da[,1:50]))


#' random access is slower than by blocks
system.time(as.matrix(da[,sample(1:ncol(da), 10)]))
system.time(as.matrix(da[,sample(1:ncol(da), 25)]))


#'
#' # Use case I: Particular rows for 1 column
#'

#' * faster loading time with less but random rows?
#+ multi row and single col
system.time(as.matrix(da[,1]))
system.time(as.matrix(da[sample(1:nrow(da), 1e3),1]))
system.time(as.matrix(da[sample(1:nrow(da), 1e4),1]))

#' If the row subset is faster, you could spend more time in a clever
#' row selection.
#' For us this could be no missing values, no Pvalues>0.5, ...,
#' to reduce the number of points we plot in the end.
#' Typical size would be 5k to 10k points to plot.
#'


#'
#' # Use case II: Particular rows for multiple columns
#'
#' Extract multiple features (organized in different HDF5 files)
#' for 1 patient (1 column per file),
#' but only the relevant rows (random indices).
#'


#' * faster loading time with less but random rows?
#+ multi row and col
system.time(as.matrix(da[,1:10]))
system.time(as.matrix(da[sample(1:nrow(da), 1e3), 1:10]))
system.time(as.matrix(da[sample(1:nrow(da), 1e3), sample(1:ncol(da), 10)]))
system.time(as.matrix(da[sample(1:nrow(da), 1e4), 1:10]))

})


print(total.time)