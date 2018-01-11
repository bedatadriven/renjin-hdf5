package org.renjin.hdf5.groups;

import org.renjin.hdf5.DataObject;

import java.io.IOException;

public interface GroupIndex {

  DataObject getObject(String name) throws IOException;
}
