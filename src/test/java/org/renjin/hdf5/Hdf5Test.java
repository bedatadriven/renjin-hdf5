package org.renjin.hdf5;

import org.junit.Ignore;
import org.junit.Test;
import org.renjin.hdf5.chunked.Chunk;
import org.renjin.hdf5.chunked.ChunkIndex;
import org.renjin.repackaged.guava.io.Resources;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 *
 */
@Ignore
public class Hdf5Test {

  @Ignore
  @Test
  public void openAllFiles() throws IOException {

    File testFile = testFile("h5ex_d_alloc.h5");
    File testDir = testFile.getParentFile();

    for (File file : testDir.listFiles()) {
      if(file.getName().endsWith(".h5")) {
        System.out.println(file);
        Hdf5File hdf5File = new Hdf5File(file);

      }
    }

  }

  @Ignore("WIP")
  @Test
  public void tenx() throws IOException {
    Hdf5File hdf5File = new Hdf5File(new File("/home/alex/dev/renjin-benchmarks/bioinformatics/tenx/tenx_uncompressed_old.h5"));
    DataObject object = hdf5File.getObject("mm10", "data");
    ChunkIndex chunkIndex = hdf5File.openChunkIndex(object);

    Chunk chunk = chunkIndex.chunkAt(new long[] { 0 });
    assertThat(chunk.getDoubleAt(0), equalTo(1.0));
    assertThat(chunk.getDoubleAt(1), equalTo(19.0));
    assertThat(chunk.getDoubleAt(2), equalTo(14.0));
    assertThat(chunk.getDoubleAt(3), equalTo(40.0));
    assertThat(chunk.getDoubleAt(4), equalTo(29.0));
    assertThat(chunk.getDoubleAt(5), equalTo(1.0));
    assertThat(chunk.getDoubleAt(6), equalTo(17.0));
    assertThat(chunk.getDoubleAt(7), equalTo(26.0));

  }


  @Test
  public void commit() throws IOException {
    Hdf5File hdf5File = new Hdf5File(testFile("h5ex_t_commit.h5"));

    DataObject sensorType = hdf5File.getObject("Sensor_Type");


  }

  private File testFile(String resourceName) {
    return new File(Resources.getResource(resourceName).getFile());
  }

}
