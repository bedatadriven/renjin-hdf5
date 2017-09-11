import org.junit.Test;
import org.renjin.hdf5.DataObject;
import org.renjin.hdf5.Hdf5File;
import org.renjin.hdf5.chunked.ChunkTree;
import org.renjin.hdf5.message.DataLayoutMessage;
import org.renjin.hdf5.message.DataspaceMessage;
import org.renjin.hdf5.message.DatatypeMessage;

import java.io.File;
import java.io.IOException;

public class BasicTest {

    @Test
    public void myReaderTest() throws IOException {
        Hdf5File hdf5 = new Hdf5File(new File("/home/alex/Downloads/zscore_psiSite.h5"));
        System.out.println(hdf5.getRootObjects());

        DataObject object = hdf5.getObject("zscore_psiSite");
        DataspaceMessage dataspace = object.getMessage(DataspaceMessage.class);
        System.out.println(dataspace);
        DatatypeMessage datatype = object.getMessage(DatatypeMessage.class);
        System.out.println(datatype.getDataClass());

        DataLayoutMessage layout = object.getMessage(DataLayoutMessage.class);

        ChunkTree chunkTree = hdf5.openChunkTree(layout);


        System.out.println(chunkTree);

    }
}
