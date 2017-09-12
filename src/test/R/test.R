
library("org.renjin:hdf5")
library(hamcrest)

test.real.matrix <- function() {

    da <- HDF5Array("matrix.h5", "x")

    assertThat(dim(da), identicalTo(c(20L, 5000L)))
}

todo.test.int.matrix <- function() {

    da <- HDF5Array("integer.h5", "y")

    assertThat(dim(da), identicalTo(c(300L, 400L)))
    assertThat(typeof(da), identicalTo("integer"))
}